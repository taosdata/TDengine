#include "sclfunc.h"
#include "sclvector.h"

static void assignBasicParaInfo(struct SScalarParam* dst, const struct SScalarParam* src) {
  dst->type = src->type;
  dst->bytes = src->bytes;
  //dst->num = src->num;
}

/** Math functions **/
int32_t absFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  assignBasicParaInfo(pOutput, pInput);
  if (inputNum != 1 || !IS_NUMERIC_TYPE(pInput->type)) {
    return TSDB_CODE_FAILED;
  }

  char *input = NULL, *output = NULL;
  for (int32_t i = 0; i < pOutput->num; ++i) {
    if (pInput->num == 1) {
      input = pInput->data;
    } else {
      input = pInput->data + i * pInput->bytes;
    }
    output = pOutput->data + i * pOutput->bytes;

    if (isNull(input, pInput->type)) {
      setNull(output, pOutput->type, pOutput->bytes);
      continue;
    }

    switch (pInput->type) {
      case TSDB_DATA_TYPE_FLOAT: {
        float v;
        GET_TYPED_DATA(v, float, pInput->type, input);
        float result;
        result = (v > 0) ? v : -v;
        SET_TYPED_DATA(output, pOutput->type, result);
        break;
      }

      case TSDB_DATA_TYPE_DOUBLE: {
        double v;
        GET_TYPED_DATA(v, double, pInput->type, input);
        double result;
        result = (v > 0) ? v : -v;
        SET_TYPED_DATA(output, pOutput->type, result);
        break;
      }

      case TSDB_DATA_TYPE_TINYINT: {
        int8_t v;
        GET_TYPED_DATA(v, int8_t, pInput->type, input);
        int8_t result;
        result = (v > 0) ? v : -v;
        SET_TYPED_DATA(output, pOutput->type, result);
        break;
      }

      case TSDB_DATA_TYPE_SMALLINT: {
        int16_t v;
        GET_TYPED_DATA(v, int16_t, pInput->type, input);
        int16_t result;
        result = (v > 0) ? v : -v;
        SET_TYPED_DATA(output, pOutput->type, result);
        break;
      }

      case TSDB_DATA_TYPE_INT: {
        int32_t v;
        GET_TYPED_DATA(v, int32_t, pInput->type, input);
        int32_t result;
        result = (v > 0) ? v : -v;
        SET_TYPED_DATA(output, pOutput->type, result);
        break;
      }

      case TSDB_DATA_TYPE_BIGINT: {
        int64_t v;
        GET_TYPED_DATA(v, int64_t, pInput->type, input);
        int64_t result;
        result = (v > 0) ? v : -v;
        SET_TYPED_DATA(output, pOutput->type, result);
        break;
      }

      default: {
        memcpy(output, input, pInput->bytes);
        break;
      }
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t logFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  if (inputNum != 2 || !IS_NUMERIC_TYPE(pInput[0].type) || !IS_NUMERIC_TYPE(pInput[1].type)) {
    return TSDB_CODE_FAILED;
  }

  pOutput->type = TSDB_DATA_TYPE_DOUBLE;
  pOutput->bytes = tDataTypes[TSDB_DATA_TYPE_DOUBLE].bytes;

  char **input = NULL, *output = NULL;
  bool hasNullInput = false;
  input = taosMemoryCalloc(inputNum, sizeof(char *));
  for (int32_t i = 0; i < pOutput->num; ++i) {
    for (int32_t j = 0; j < inputNum; ++j) {
      if (pInput[j].num == 1) {
        input[j] = pInput[j].data;
      } else {
        input[j] = pInput[j].data + i * pInput[j].bytes;
      }
      if (isNull(input[j], pInput[j].type)) {
        hasNullInput = true;
        break;
      }
    }
    output = pOutput->data + i * pOutput->bytes;

    if (hasNullInput) {
      setNull(output, pOutput->type, pOutput->bytes);
      continue;
    }

    double base;
    GET_TYPED_DATA(base, double, pInput[1].type, input[1]);
    double v;
    GET_TYPED_DATA(v, double, pInput[0].type, input[0]);
    double result = log(v) / log(base);
    SET_TYPED_DATA(output, pOutput->type, result);
  }

  taosMemoryFree(input);

  return TSDB_CODE_SUCCESS;
}

int32_t powFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  if (inputNum != 2 || !IS_NUMERIC_TYPE(pInput[0].type) || !IS_NUMERIC_TYPE(pInput[1].type)) {
    return TSDB_CODE_FAILED;
  }

  pOutput->type = TSDB_DATA_TYPE_DOUBLE;
  pOutput->bytes = tDataTypes[TSDB_DATA_TYPE_DOUBLE].bytes;

  char **input = NULL, *output = NULL;
  bool hasNullInput = false;
  input = taosMemoryCalloc(inputNum, sizeof(char *));
  for (int32_t i = 0; i < pOutput->num; ++i) {
    for (int32_t j = 0; j < inputNum; ++j) {
      if (pInput[j].num == 1) {
        input[j] = pInput[j].data;
      } else {
        input[j] = pInput[j].data + i * pInput[j].bytes;
      }
      if (isNull(input[j], pInput[j].type)) {
        hasNullInput = true;
        break;
      }
    }
    output = pOutput->data + i * pOutput->bytes;

    if (hasNullInput) {
      setNull(output, pOutput->type, pOutput->bytes);
      continue;
    }

    double base;
    GET_TYPED_DATA(base, double, pInput[1].type, input[1]);
    double v;
    GET_TYPED_DATA(v, double, pInput[0].type, input[0]);
    double result = pow(v, base);
    SET_TYPED_DATA(output, pOutput->type, result);
  }

  taosMemoryFree(input);

  return TSDB_CODE_SUCCESS;
}

int32_t sqrtFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  if (inputNum != 1 || !IS_NUMERIC_TYPE(pInput->type)) {
    return TSDB_CODE_FAILED;
  }

  pOutput->type = TSDB_DATA_TYPE_DOUBLE;
  pOutput->bytes = tDataTypes[TSDB_DATA_TYPE_DOUBLE].bytes;

  char *input = NULL, *output = NULL;
  for (int32_t i = 0; i < pOutput->num; ++i) {
    if (pInput->num == 1) {
      input = pInput->data;
    } else {
      input = pInput->data + i * pInput->bytes;
    }
    output = pOutput->data + i * pOutput->bytes;

    if (isNull(input, pInput->type)) {
      setNull(output, pOutput->type, pOutput->bytes);
      continue;
    }

    double v;
    GET_TYPED_DATA(v, double, pInput->type, input);
    double result = sqrt(v);
    SET_TYPED_DATA(output, pOutput->type, result);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t sinFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  if (inputNum != 1 || !IS_NUMERIC_TYPE(pInput->type)) {
    return TSDB_CODE_FAILED;
  }

  pOutput->type = TSDB_DATA_TYPE_DOUBLE;
  pOutput->bytes = tDataTypes[TSDB_DATA_TYPE_DOUBLE].bytes;

  char *input = NULL, *output = NULL;
  for (int32_t i = 0; i < pOutput->num; ++i) {
    if (pInput->num == 1) {
      input = pInput->data;
    } else {
      input = pInput->data + i * pInput->bytes;
    }
    output = pOutput->data + i * pOutput->bytes;

    if (isNull(input, pInput->type)) {
      setNull(output, pOutput->type, pOutput->bytes);
      continue;
    }

    double v;
    GET_TYPED_DATA(v, double, pInput->type, input);
    double result = sin(v);
    SET_TYPED_DATA(output, pOutput->type, result);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t cosFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  if (inputNum != 1 || !IS_NUMERIC_TYPE(pInput->type)) {
    return TSDB_CODE_FAILED;
  }

  pOutput->type = TSDB_DATA_TYPE_DOUBLE;
  pOutput->bytes = tDataTypes[TSDB_DATA_TYPE_DOUBLE].bytes;

  char *input = NULL, *output = NULL;
  for (int32_t i = 0; i < pOutput->num; ++i) {
    if (pInput->num == 1) {
      input = pInput->data;
    } else {
      input = pInput->data + i * pInput->bytes;
    }
    output = pOutput->data + i * pOutput->bytes;

    if (isNull(input, pInput->type)) {
      setNull(output, pOutput->type, pOutput->bytes);
      continue;
    }

    double v;
    GET_TYPED_DATA(v, double, pInput->type, input);
    double result = cos(v);
    SET_TYPED_DATA(output, pOutput->type, result);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t tanFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  if (inputNum != 1 || !IS_NUMERIC_TYPE(pInput->type)) {
    return TSDB_CODE_FAILED;
  }

  pOutput->type = TSDB_DATA_TYPE_DOUBLE;
  pOutput->bytes = tDataTypes[TSDB_DATA_TYPE_DOUBLE].bytes;

  char *input = NULL, *output = NULL;
  for (int32_t i = 0; i < pOutput->num; ++i) {
    if (pInput->num == 1) {
      input = pInput->data;
    } else {
      input = pInput->data + i * pInput->bytes;
    }
    output = pOutput->data + i * pOutput->bytes;

    if (isNull(input, pInput->type)) {
      setNull(output, pOutput->type, pOutput->bytes);
      continue;
    }

    double v;
    GET_TYPED_DATA(v, double, pInput->type, input);
    double result = tan(v);
    SET_TYPED_DATA(output, pOutput->type, result);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t asinFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  if (inputNum != 1 || !IS_NUMERIC_TYPE(pInput->type)) {
    return TSDB_CODE_FAILED;
  }

  pOutput->type = TSDB_DATA_TYPE_DOUBLE;
  pOutput->bytes = tDataTypes[TSDB_DATA_TYPE_DOUBLE].bytes;

  char *input = NULL, *output = NULL;
  for (int32_t i = 0; i < pOutput->num; ++i) {
    if (pInput->num == 1) {
      input = pInput->data;
    } else {
      input = pInput->data + i * pInput->bytes;
    }
    output = pOutput->data + i * pOutput->bytes;

    if (isNull(input, pInput->type)) {
      setNull(output, pOutput->type, pOutput->bytes);
      continue;
    }

    double v;
    GET_TYPED_DATA(v, double, pInput->type, input);
    double result = asin(v);
    SET_TYPED_DATA(output, pOutput->type, result);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t acosFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  if (inputNum != 1 || !IS_NUMERIC_TYPE(pInput->type)) {
    return TSDB_CODE_FAILED;
  }

  pOutput->type = TSDB_DATA_TYPE_DOUBLE;
  pOutput->bytes = tDataTypes[TSDB_DATA_TYPE_DOUBLE].bytes;

  char *input = NULL, *output = NULL;
  for (int32_t i = 0; i < pOutput->num; ++i) {
    if (pInput->num == 1) {
      input = pInput->data;
    } else {
      input = pInput->data + i * pInput->bytes;
    }
    output = pOutput->data + i * pOutput->bytes;

    if (isNull(input, pInput->type)) {
      setNull(output, pOutput->type, pOutput->bytes);
      continue;
    }

    double v;
    GET_TYPED_DATA(v, double, pInput->type, input);
    double result = acos(v);
    SET_TYPED_DATA(output, pOutput->type, result);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t atanFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  if (inputNum != 1 || !IS_NUMERIC_TYPE(pInput->type)) {
    return TSDB_CODE_FAILED;
  }

  pOutput->type = TSDB_DATA_TYPE_DOUBLE;
  pOutput->bytes = tDataTypes[TSDB_DATA_TYPE_DOUBLE].bytes;

  char *input = NULL, *output = NULL;
  for (int32_t i = 0; i < pOutput->num; ++i) {
    if (pInput->num == 1) {
      input = pInput->data;
    } else {
      input = pInput->data + i * pInput->bytes;
    }
    output = pOutput->data + i * pOutput->bytes;

    if (isNull(input, pInput->type)) {
      setNull(output, pOutput->type, pOutput->bytes);
      continue;
    }

    double v;
    GET_TYPED_DATA(v, double, pInput->type, input);
    double result = atan(v);
    SET_TYPED_DATA(output, pOutput->type, result);
  }

  return TSDB_CODE_SUCCESS;
}

int32_t ceilFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  if (inputNum != 1 || !IS_NUMERIC_TYPE(pInput->type)) {
    return TSDB_CODE_FAILED;
  }

  char *input = NULL, *output = NULL;
  for (int32_t i = 0; i < pOutput->num; ++i) {
    if (pInput->num == 1) {
      input = pInput->data;
    } else {
      input = pInput->data + i * pInput->bytes;
    }
    output = pOutput->data + i * pOutput->bytes;

    if (isNull(input, pInput->type)) {
      setNull(output, pOutput->type, pOutput->bytes);
      continue;
    }

    switch (pInput->type) {
      case TSDB_DATA_TYPE_FLOAT: {
        float v;
        GET_TYPED_DATA(v, float, pInput->type, input);
        float result = ceilf(v);
        SET_TYPED_DATA(output, pOutput->type, result);
        break;
      }

      case TSDB_DATA_TYPE_DOUBLE: {
        double v;
        GET_TYPED_DATA(v, double, pInput->type, input);
        double result = ceil(v);
        SET_TYPED_DATA(output, pOutput->type, result);
        break;
      }

      default: {
        memcpy(output, input, pInput->bytes);
        break;
      }
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t floorFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  assignBasicParaInfo(pOutput, pInput);
  if (inputNum != 1 || !IS_NUMERIC_TYPE(pInput->type)) {
    return TSDB_CODE_FAILED;
  }

  char *input = NULL, *output = NULL;
  for (int32_t i = 0; i < pOutput->num; ++i) {
    if (pInput->num == 1) {
      input = pInput->data;
    } else {
      input = pInput->data + i * pInput->bytes;
    }
    output = pOutput->data + i * pOutput->bytes;

    if (isNull(input, pInput->type)) {
      setNull(output, pOutput->type, pOutput->bytes);
      continue;
    }

    switch (pInput->type) {
      case TSDB_DATA_TYPE_FLOAT: {
        float v;
        GET_TYPED_DATA(v, float, pInput->type, input);
        float result = floorf(v);
        SET_TYPED_DATA(output, pOutput->type, result);
        break;
      }

      case TSDB_DATA_TYPE_DOUBLE: {
        double v;
        GET_TYPED_DATA(v, double, pInput->type, input);
        double result = floor(v);
        SET_TYPED_DATA(output, pOutput->type, result);
        break;
      }

      default: {
        memcpy(output, input, pInput->bytes);
        break;
      }
    }
  }

  return TSDB_CODE_SUCCESS;
}

int32_t roundFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  assignBasicParaInfo(pOutput, pInput);
  if (inputNum != 1 || !IS_NUMERIC_TYPE(pInput->type)) {
    return TSDB_CODE_FAILED;
  }

  char *input = NULL, *output = NULL;
  for (int32_t i = 0; i < pOutput->num; ++i) {
    if (pInput->num == 1) {
      input = pInput->data;
    } else {
      input = pInput->data + i * pInput->bytes;
    }
    output = pOutput->data + i * pOutput->bytes;

    if (isNull(input, pInput->type)) {
      setNull(output, pOutput->type, pOutput->bytes);
      continue;
    }

    switch (pInput->type) {
      case TSDB_DATA_TYPE_FLOAT: {
        float v;
        GET_TYPED_DATA(v, float, pInput->type, input);
        float result = roundf(v);
        SET_TYPED_DATA(output, pOutput->type, result);
        break;
      }

      case TSDB_DATA_TYPE_DOUBLE: {
        double v;
        GET_TYPED_DATA(v, double, pInput->type, input);
        double result = round(v);
        SET_TYPED_DATA(output, pOutput->type, result);
        break;
      }

      default: {
        memcpy(output, input, pInput->bytes);
        break;
      }
    }
  }

  return TSDB_CODE_SUCCESS;
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

  pOutput->data = taosMemoryRealloc(pOutput->data, rowLen * num);
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
    leftOutput.data = taosMemoryMalloc(sizeof(int64_t) * numOfRows);
    evaluateExprNodeTree(pLeft, numOfRows, &leftOutput, param, getSourceDataBlock);
  }

  // the right output has result from the right child syntax tree
  if (pRight->nodeType == TEXPR_BINARYEXPR_NODE || pRight->nodeType == TEXPR_UNARYEXPR_NODE) {
    rightOutput.data = taosMemoryMalloc(sizeof(int64_t) * numOfRows);
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
      outputBuf = taosMemoryRealloc(pOutput->data, (left.bytes + right.bytes) * left.num);
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
      void* outputBuf = taosMemoryRealloc(pOutput->data, left.bytes * left.num);
      assert(outputBuf != NULL);
      pOutput->data = outputBuf;
    }

    OperatorFn(&left, pOutput);
  }

  taosMemoryFreeClear(leftOutput.data);
  taosMemoryFreeClear(rightOutput.data);

  return 0;
}
#endif


//SScalarFunctionInfo scalarFunc[8] = {
//    {"ceil",   FUNCTION_TYPE_SCALAR, FUNCTION_CEIL,   tceil},
//    {"floor",  FUNCTION_TYPE_SCALAR, FUNCTION_FLOOR,  tfloor},
//    {"abs",    FUNCTION_TYPE_SCALAR, FUNCTION_ABS,    _tabs},
//    {"round",  FUNCTION_TYPE_SCALAR, FUNCTION_ROUND,  tround},
//    {"length", FUNCTION_TYPE_SCALAR, FUNCTION_LENGTH, tlength},
//    {"concat", FUNCTION_TYPE_SCALAR, FUNCTION_CONCAT, tconcat},
//    {"ltrim",  FUNCTION_TYPE_SCALAR, FUNCTION_LTRIM, tltrim},
//    {"rtrim",  FUNCTION_TYPE_SCALAR, FUNCTION_RTRIM, trtrim},
//};

void setScalarFunctionSupp(struct SScalarFunctionSupport* sas, SExprInfo *pExprInfo, SSDataBlock* pSDataBlock) {
  sas->numOfCols = (int32_t) pSDataBlock->info.numOfCols;
  sas->pExprInfo = pExprInfo;
  if (sas->colList != NULL) {
    return;
  }

  sas->colList = taosMemoryCalloc(1, pSDataBlock->info.numOfCols*sizeof(SColumnInfo));
  for(int32_t i = 0; i < sas->numOfCols; ++i) {
    SColumnInfoData* pColData = taosArrayGet(pSDataBlock->pDataBlock, i);
    sas->colList[i] = pColData->info;
  }

  sas->data = taosMemoryCalloc(sas->numOfCols, POINTER_BYTES);

  // set the input column data
  for (int32_t f = 0; f < pSDataBlock->info.numOfCols; ++f) {
    SColumnInfoData *pColumnInfoData = taosArrayGet(pSDataBlock->pDataBlock, f);
    sas->data[f] = pColumnInfoData->pData;
  }
}

SScalarFunctionSupport* createScalarFuncSupport(int32_t num) {
  SScalarFunctionSupport* pSupp = taosMemoryCalloc(num, sizeof(SScalarFunctionSupport));
  return pSupp;
}

void destroyScalarFuncSupport(struct SScalarFunctionSupport* pSupport, int32_t num) {
  if (pSupport == NULL) {
    return;
  }

  for(int32_t i = 0; i < num; ++i) {
    SScalarFunctionSupport* pSupp = &pSupport[i];
    taosMemoryFreeClear(pSupp->data);
    taosMemoryFreeClear(pSupp->colList);
  }

  taosMemoryFreeClear(pSupport);
}
