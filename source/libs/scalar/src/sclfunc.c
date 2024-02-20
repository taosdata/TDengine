#include "cJSON.h"
#include "function.h"
#include "scalar.h"
#include "sclInt.h"
#include "sclvector.h"
#include "tdatablock.h"
#include "tjson.h"
#include "ttime.h"

typedef float (*_float_fn)(float);
typedef double (*_double_fn)(double);
typedef double (*_double_fn_2)(double, double);
typedef int (*_conv_fn)(int);
typedef void (*_trim_fn)(char *, char *, int32_t, int32_t);
typedef uint16_t (*_len_fn)(char *, int32_t);

/** Math functions **/
static double tlog(double v) { return log(v); }

static double tlog2(double v, double base) {
  double a = log(v);
  double b = log(base);
  if (isnan(a) || isinf(a)) {
    return a;
  } else if (isnan(b) || isinf(b)) {
    return b;
  } else if (b == 0) {
    return INFINITY;
  } else {
    return a / b;
  }
}

int32_t absFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  SColumnInfoData *pInputData = pInput->columnData;
  SColumnInfoData *pOutputData = pOutput->columnData;

  int32_t type = GET_PARAM_TYPE(pInput);

  switch (type) {
    case TSDB_DATA_TYPE_FLOAT: {
      float *in = (float *)pInputData->pData;
      float *out = (float *)pOutputData->pData;
      for (int32_t i = 0; i < pInput->numOfRows; ++i) {
        if (colDataIsNull_s(pInputData, i)) {
          colDataSetNULL(pOutputData, i);
          continue;
        }
        out[i] = (in[i] >= 0) ? in[i] : -in[i];
      }
      break;
    }

    case TSDB_DATA_TYPE_DOUBLE: {
      double *in = (double *)pInputData->pData;
      double *out = (double *)pOutputData->pData;
      for (int32_t i = 0; i < pInput->numOfRows; ++i) {
        if (colDataIsNull_s(pInputData, i)) {
          colDataSetNULL(pOutputData, i);
          continue;
        }
        out[i] = (in[i] >= 0) ? in[i] : -in[i];
      }
      break;
    }

    case TSDB_DATA_TYPE_TINYINT: {
      int8_t *in = (int8_t *)pInputData->pData;
      int8_t *out = (int8_t *)pOutputData->pData;
      for (int32_t i = 0; i < pInput->numOfRows; ++i) {
        if (colDataIsNull_s(pInputData, i)) {
          colDataSetNULL(pOutputData, i);
          continue;
        }
        out[i] = (in[i] >= 0) ? in[i] : -in[i];
      }
      break;
    }

    case TSDB_DATA_TYPE_SMALLINT: {
      int16_t *in = (int16_t *)pInputData->pData;
      int16_t *out = (int16_t *)pOutputData->pData;
      for (int32_t i = 0; i < pInput->numOfRows; ++i) {
        if (colDataIsNull_s(pInputData, i)) {
          colDataSetNULL(pOutputData, i);
          continue;
        }
        out[i] = (in[i] >= 0) ? in[i] : -in[i];
      }
      break;
    }

    case TSDB_DATA_TYPE_INT: {
      int32_t *in = (int32_t *)pInputData->pData;
      int32_t *out = (int32_t *)pOutputData->pData;
      for (int32_t i = 0; i < pInput->numOfRows; ++i) {
        if (colDataIsNull_s(pInputData, i)) {
          colDataSetNULL(pOutputData, i);
          continue;
        }
        out[i] = (in[i] >= 0) ? in[i] : -in[i];
      }
      break;
    }

    case TSDB_DATA_TYPE_BIGINT: {
      int64_t *in = (int64_t *)pInputData->pData;
      int64_t *out = (int64_t *)pOutputData->pData;
      for (int32_t i = 0; i < pInput->numOfRows; ++i) {
        if (colDataIsNull_s(pInputData, i)) {
          colDataSetNULL(pOutputData, i);
          continue;
        }
        out[i] = (in[i] >= 0) ? in[i] : -in[i];
      }
      break;
    }

    case TSDB_DATA_TYPE_NULL: {
      for (int32_t i = 0; i < pInput->numOfRows; ++i) {
        colDataSetNULL(pOutputData, i);
      }
      break;
    }

    default: {
      colDataAssign(pOutputData, pInputData, pInput->numOfRows, NULL);
    }
  }

  pOutput->numOfRows = pInput->numOfRows;
  return TSDB_CODE_SUCCESS;
}

static int32_t doScalarFunctionUnique(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput, _double_fn valFn) {
  int32_t type = GET_PARAM_TYPE(pInput);

  SColumnInfoData *pInputData = pInput->columnData;
  SColumnInfoData *pOutputData = pOutput->columnData;

  _getDoubleValue_fn_t getValueFn = getVectorDoubleValueFn(type);

  double *out = (double *)pOutputData->pData;

  for (int32_t i = 0; i < pInput->numOfRows; ++i) {
    if (colDataIsNull_s(pInputData, i) || IS_NULL_TYPE(type)) {
      colDataSetNULL(pOutputData, i);
      continue;
    }
    double result = valFn(getValueFn(pInputData->pData, i));
    if (isinf(result) || isnan(result)) {
      colDataSetNULL(pOutputData, i);
    } else {
      out[i] = result;
    }
  }

  pOutput->numOfRows = pInput->numOfRows;
  return TSDB_CODE_SUCCESS;
}

static int32_t doScalarFunctionUnique2(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput,
                                       _double_fn_2 valFn) {
  SColumnInfoData     *pInputData[2];
  SColumnInfoData     *pOutputData = pOutput->columnData;
  _getDoubleValue_fn_t getValueFn[2];

  for (int32_t i = 0; i < inputNum; ++i) {
    pInputData[i] = pInput[i].columnData;
    getValueFn[i] = getVectorDoubleValueFn(GET_PARAM_TYPE(&pInput[i]));
  }

  double *out = (double *)pOutputData->pData;
  double  result;

  bool hasNullType = (IS_NULL_TYPE(GET_PARAM_TYPE(&pInput[0])) || IS_NULL_TYPE(GET_PARAM_TYPE(&pInput[1])));

  int32_t numOfRows = TMAX(pInput[0].numOfRows, pInput[1].numOfRows);
  if (pInput[0].numOfRows == pInput[1].numOfRows) {
    for (int32_t i = 0; i < numOfRows; ++i) {
      if (colDataIsNull_s(pInputData[0], i) || colDataIsNull_s(pInputData[1], i) || hasNullType) {
        colDataSetNULL(pOutputData, i);
        continue;
      }
      result = valFn(getValueFn[0](pInputData[0]->pData, i), getValueFn[1](pInputData[1]->pData, i));
      if (isinf(result) || isnan(result)) {
        colDataSetNULL(pOutputData, i);
      } else {
        out[i] = result;
      }
    }
  } else if (pInput[0].numOfRows == 1) {  // left operand is constant
    if (colDataIsNull_s(pInputData[0], 0) || hasNullType) {
      colDataSetNNULL(pOutputData, 0, pInput[1].numOfRows);
    } else {
      for (int32_t i = 0; i < numOfRows; ++i) {
        if (colDataIsNull_s(pInputData[1], i)) {
          colDataSetNULL(pOutputData, i);
          continue;
        }

        result = valFn(getValueFn[0](pInputData[0]->pData, 0), getValueFn[1](pInputData[1]->pData, i));
        if (isinf(result) || isnan(result)) {
          colDataSetNULL(pOutputData, i);
          continue;
        }

        out[i] = result;
      }
    }
  } else if (pInput[1].numOfRows == 1) {
    if (colDataIsNull_s(pInputData[1], 0) || hasNullType) {
      colDataSetNNULL(pOutputData, 0, pInput[0].numOfRows);
    } else {
      for (int32_t i = 0; i < numOfRows; ++i) {
        if (colDataIsNull_s(pInputData[0], i)) {
          colDataSetNULL(pOutputData, i);
          continue;
        }

        result = valFn(getValueFn[0](pInputData[0]->pData, i), getValueFn[1](pInputData[1]->pData, 0));
        if (isinf(result) || isnan(result)) {
          colDataSetNULL(pOutputData, i);
          continue;
        }

        out[i] = result;
      }
    }
  }

  pOutput->numOfRows = numOfRows;
  return TSDB_CODE_SUCCESS;
}

static int32_t doScalarFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput, _float_fn f1,
                                _double_fn d1) {
  int32_t type = GET_PARAM_TYPE(pInput);

  SColumnInfoData *pInputData = pInput->columnData;
  SColumnInfoData *pOutputData = pOutput->columnData;

  switch (type) {
    case TSDB_DATA_TYPE_FLOAT: {
      float *in = (float *)pInputData->pData;
      float *out = (float *)pOutputData->pData;

      for (int32_t i = 0; i < pInput->numOfRows; ++i) {
        if (colDataIsNull_s(pInputData, i)) {
          colDataSetNULL(pOutputData, i);
          continue;
        }
        out[i] = f1(in[i]);
      }
      break;
    }

    case TSDB_DATA_TYPE_DOUBLE: {
      double *in = (double *)pInputData->pData;
      double *out = (double *)pOutputData->pData;

      for (int32_t i = 0; i < pInput->numOfRows; ++i) {
        if (colDataIsNull_s(pInputData, i)) {
          colDataSetNULL(pOutputData, i);
          continue;
        }
        out[i] = d1(in[i]);
      }
      break;
    }

    case TSDB_DATA_TYPE_NULL: {
      for (int32_t i = 0; i < pInput->numOfRows; ++i) {
        colDataSetNULL(pOutputData, i);
      }
      break;
    }

    default: {
      colDataAssign(pOutputData, pInputData, pInput->numOfRows, NULL);
    }
  }

  pOutput->numOfRows = pInput->numOfRows;
  return TSDB_CODE_SUCCESS;
}

/** String functions **/
static VarDataLenT tlength(char *input, int32_t type) { return varDataLen(input); }

static VarDataLenT tcharlength(char *input, int32_t type) {
  if (type == TSDB_DATA_TYPE_VARCHAR || type == TSDB_DATA_TYPE_GEOMETRY) {
    return varDataLen(input);
  } else {  // NCHAR
    return varDataLen(input) / TSDB_NCHAR_SIZE;
  }
}

static void tltrim(char *input, char *output, int32_t type, int32_t charLen) {
  int32_t numOfSpaces = 0;
  if (type == TSDB_DATA_TYPE_VARCHAR) {
    for (int32_t i = 0; i < charLen; ++i) {
      if (!isspace(*(varDataVal(input) + i))) {
        break;
      }
      numOfSpaces++;
    }
  } else {  // NCHAR
    for (int32_t i = 0; i < charLen; ++i) {
      if (!iswspace(*((uint32_t *)varDataVal(input) + i))) {
        break;
      }
      numOfSpaces++;
    }
  }

  int32_t resLen;
  if (type == TSDB_DATA_TYPE_VARCHAR) {
    resLen = charLen - numOfSpaces;
    memcpy(varDataVal(output), varDataVal(input) + numOfSpaces, resLen);
  } else {
    resLen = (charLen - numOfSpaces) * TSDB_NCHAR_SIZE;
    memcpy(varDataVal(output), varDataVal(input) + numOfSpaces * TSDB_NCHAR_SIZE, resLen);
  }

  varDataSetLen(output, resLen);
}

static void trtrim(char *input, char *output, int32_t type, int32_t charLen) {
  int32_t numOfSpaces = 0;
  if (type == TSDB_DATA_TYPE_VARCHAR) {
    for (int32_t i = charLen - 1; i >= 0; --i) {
      if (!isspace(*(varDataVal(input) + i))) {
        break;
      }
      numOfSpaces++;
    }
  } else {  // NCHAR
    for (int32_t i = charLen - 1; i >= 0; --i) {
      if (!iswspace(*((uint32_t *)varDataVal(input) + i))) {
        break;
      }
      numOfSpaces++;
    }
  }

  int32_t resLen;
  if (type == TSDB_DATA_TYPE_VARCHAR) {
    resLen = charLen - numOfSpaces;
  } else {
    resLen = (charLen - numOfSpaces) * TSDB_NCHAR_SIZE;
  }
  memcpy(varDataVal(output), varDataVal(input), resLen);

  varDataSetLen(output, resLen);
}

static int32_t doLengthFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput, _len_fn lenFn) {
  int32_t type = GET_PARAM_TYPE(pInput);

  SColumnInfoData *pInputData = pInput->columnData;
  SColumnInfoData *pOutputData = pOutput->columnData;

  int64_t *out = (int64_t *)pOutputData->pData;

  for (int32_t i = 0; i < pInput->numOfRows; ++i) {
    if (colDataIsNull_s(pInputData, i)) {
      colDataSetNULL(pOutputData, i);
      continue;
    }

    char *in = colDataGetData(pInputData, i);
    out[i] = lenFn(in, type);
  }

  pOutput->numOfRows = pInput->numOfRows;
  return TSDB_CODE_SUCCESS;
}

static int32_t concatCopyHelper(const char *input, char *output, bool hasNchar, int32_t type, VarDataLenT *dataLen) {
  if (hasNchar && type == TSDB_DATA_TYPE_VARCHAR) {
    TdUcs4 *newBuf = taosMemoryCalloc((varDataLen(input) + 1) * TSDB_NCHAR_SIZE, 1);
    int32_t len = varDataLen(input);
    bool    ret = taosMbsToUcs4(varDataVal(input), len, newBuf, (varDataLen(input) + 1) * TSDB_NCHAR_SIZE, &len);
    if (!ret) {
      taosMemoryFree(newBuf);
      return TSDB_CODE_FAILED;
    }
    memcpy(varDataVal(output) + *dataLen, newBuf, varDataLen(input) * TSDB_NCHAR_SIZE);
    *dataLen += varDataLen(input) * TSDB_NCHAR_SIZE;
    taosMemoryFree(newBuf);
  } else {
    memcpy(varDataVal(output) + *dataLen, varDataVal(input), varDataLen(input));
    *dataLen += varDataLen(input);
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t getNumOfNullEntries(SColumnInfoData *pColumnInfoData, int32_t numOfRows) {
  int32_t numOfNulls = 0;
  if (!pColumnInfoData->hasNull) {
    return numOfNulls;
  }
  for (int i = 0; i < numOfRows; ++i) {
    if (pColumnInfoData->varmeta.offset[i] == -1) {
      numOfNulls++;
    }
  }
  return numOfNulls;
}

int32_t concatFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  int32_t           ret = TSDB_CODE_SUCCESS;
  SColumnInfoData **pInputData = taosMemoryCalloc(inputNum, sizeof(SColumnInfoData *));
  SColumnInfoData  *pOutputData = pOutput->columnData;
  char            **input = taosMemoryCalloc(inputNum, POINTER_BYTES);
  char             *outputBuf = NULL;

  int32_t inputLen = 0;
  int32_t numOfRows = 0;
  bool    hasNchar = (GET_PARAM_TYPE(pOutput) == TSDB_DATA_TYPE_NCHAR) ? true : false;
  for (int32_t i = 0; i < inputNum; ++i) {
    if (pInput[i].numOfRows > numOfRows) {
      numOfRows = pInput[i].numOfRows;
    }
  }
  for (int32_t i = 0; i < inputNum; ++i) {
    pInputData[i] = pInput[i].columnData;
    int32_t factor = 1;
    if (hasNchar && (GET_PARAM_TYPE(&pInput[i]) == TSDB_DATA_TYPE_VARCHAR)) {
      factor = TSDB_NCHAR_SIZE;
    }

    int32_t numOfNulls = getNumOfNullEntries(pInputData[i], pInput[i].numOfRows);
    if (pInput[i].numOfRows == 1) {
      inputLen += (pInputData[i]->varmeta.length - VARSTR_HEADER_SIZE) * factor * (numOfRows - numOfNulls);
    } else {
      inputLen += (pInputData[i]->varmeta.length - (numOfRows - numOfNulls) * VARSTR_HEADER_SIZE) * factor;
    }
  }

  int32_t outputLen = inputLen + numOfRows * VARSTR_HEADER_SIZE;
  outputBuf = taosMemoryCalloc(outputLen, 1);
  char *output = outputBuf;

  for (int32_t k = 0; k < numOfRows; ++k) {
    bool hasNull = false;
    for (int32_t i = 0; i < inputNum; ++i) {
      if (colDataIsNull_s(pInputData[i], k) || IS_NULL_TYPE(GET_PARAM_TYPE(&pInput[i]))) {
        colDataSetNULL(pOutputData, k);
        hasNull = true;
        break;
      }
    }

    if (hasNull) {
      continue;
    }

    VarDataLenT dataLen = 0;
    for (int32_t i = 0; i < inputNum; ++i) {
      int32_t rowIdx = (pInput[i].numOfRows == 1) ? 0 : k;
      input[i] = colDataGetData(pInputData[i], rowIdx);

      ret = concatCopyHelper(input[i], output, hasNchar, GET_PARAM_TYPE(&pInput[i]), &dataLen);
      if (ret != TSDB_CODE_SUCCESS) {
        goto DONE;
      }
    }
    varDataSetLen(output, dataLen);
    colDataSetVal(pOutputData, k, output, false);
    output += varDataTLen(output);
  }

  pOutput->numOfRows = numOfRows;

DONE:
  taosMemoryFree(input);
  taosMemoryFree(outputBuf);
  taosMemoryFree(pInputData);

  return ret;
}

int32_t concatWsFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  int32_t           ret = TSDB_CODE_SUCCESS;
  SColumnInfoData **pInputData = taosMemoryCalloc(inputNum, sizeof(SColumnInfoData *));
  SColumnInfoData  *pOutputData = pOutput->columnData;
  char            **input = taosMemoryCalloc(inputNum, POINTER_BYTES);
  char             *outputBuf = NULL;

  int32_t inputLen = 0;
  int32_t numOfRows = 0;
  bool    hasNchar = (GET_PARAM_TYPE(pOutput) == TSDB_DATA_TYPE_NCHAR) ? true : false;
  for (int32_t i = 1; i < inputNum; ++i) {
    if (pInput[i].numOfRows > numOfRows) {
      numOfRows = pInput[i].numOfRows;
    }
  }
  for (int32_t i = 0; i < inputNum; ++i) {
    pInputData[i] = pInput[i].columnData;
    int32_t factor = 1;
    if (hasNchar && (GET_PARAM_TYPE(&pInput[i]) == TSDB_DATA_TYPE_VARCHAR)) {
      factor = TSDB_NCHAR_SIZE;
    }

    int32_t numOfNulls = getNumOfNullEntries(pInputData[i], pInput[i].numOfRows);
    if (i == 0) {
      // calculate required separator space
      inputLen +=
          (pInputData[0]->varmeta.length - VARSTR_HEADER_SIZE) * (numOfRows - numOfNulls) * (inputNum - 2) * factor;
    } else if (pInput[i].numOfRows == 1) {
      inputLen += (pInputData[i]->varmeta.length - VARSTR_HEADER_SIZE) * (numOfRows - numOfNulls) * factor;
    } else {
      inputLen += (pInputData[i]->varmeta.length - (numOfRows - numOfNulls) * VARSTR_HEADER_SIZE) * factor;
    }
  }

  int32_t outputLen = inputLen + numOfRows * VARSTR_HEADER_SIZE;
  outputBuf = taosMemoryCalloc(outputLen, 1);
  char *output = outputBuf;

  for (int32_t k = 0; k < numOfRows; ++k) {
    if (colDataIsNull_s(pInputData[0], k) || IS_NULL_TYPE(GET_PARAM_TYPE(&pInput[0]))) {
      colDataSetNULL(pOutputData, k);
      continue;
    }

    VarDataLenT dataLen = 0;
    bool        hasNull = false;
    for (int32_t i = 1; i < inputNum; ++i) {
      if (colDataIsNull_s(pInputData[i], k) || IS_NULL_TYPE(GET_PARAM_TYPE(&pInput[i]))) {
        hasNull = true;
        break;
      }

      int32_t rowIdx = (pInput[i].numOfRows == 1) ? 0 : k;
      input[i] = colDataGetData(pInputData[i], rowIdx);

      ret = concatCopyHelper(input[i], output, hasNchar, GET_PARAM_TYPE(&pInput[i]), &dataLen);
      if (ret != TSDB_CODE_SUCCESS) {
        goto DONE;
      }

      if (i < inputNum - 1) {
        // insert the separator
        char *sep = (pInput[0].numOfRows == 1) ? colDataGetData(pInputData[0], 0) : colDataGetData(pInputData[0], k);
        ret = concatCopyHelper(sep, output, hasNchar, GET_PARAM_TYPE(&pInput[0]), &dataLen);
        if (ret != TSDB_CODE_SUCCESS) {
          goto DONE;
        }
      }
    }

    if (hasNull) {
      colDataSetNULL(pOutputData, k);
      memset(output, 0, dataLen);
    } else {
      varDataSetLen(output, dataLen);
      colDataSetVal(pOutputData, k, output, false);
      output += varDataTLen(output);
    }
  }

  pOutput->numOfRows = numOfRows;

DONE:
  taosMemoryFree(input);
  taosMemoryFree(outputBuf);
  taosMemoryFree(pInputData);

  return ret;
}

static int32_t doCaseConvFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput, _conv_fn convFn) {
  int32_t type = GET_PARAM_TYPE(pInput);

  SColumnInfoData *pInputData = pInput->columnData;
  SColumnInfoData *pOutputData = pOutput->columnData;

  int32_t outputLen = pInputData->varmeta.length;
  char   *outputBuf = taosMemoryCalloc(outputLen, 1);
  char   *output = outputBuf;

  for (int32_t i = 0; i < pInput->numOfRows; ++i) {
    if (colDataIsNull_s(pInputData, i)) {
      colDataSetNULL(pOutputData, i);
      continue;
    }

    char   *input = colDataGetData(pInput[0].columnData, i);
    int32_t len = varDataLen(input);
    if (type == TSDB_DATA_TYPE_VARCHAR) {
      for (int32_t j = 0; j < len; ++j) {
        *(varDataVal(output) + j) = convFn(*(varDataVal(input) + j));
      }
    } else {  // NCHAR
      for (int32_t j = 0; j < len / TSDB_NCHAR_SIZE; ++j) {
        *((uint32_t *)varDataVal(output) + j) = convFn(*((uint32_t *)varDataVal(input) + j));
      }
    }
    varDataSetLen(output, len);
    colDataSetVal(pOutputData, i, output, false);
    output += varDataTLen(output);
  }

  pOutput->numOfRows = pInput->numOfRows;
  taosMemoryFree(outputBuf);

  return TSDB_CODE_SUCCESS;
}

static int32_t doTrimFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput, _trim_fn trimFn) {
  int32_t type = GET_PARAM_TYPE(pInput);

  SColumnInfoData *pInputData = pInput->columnData;
  SColumnInfoData *pOutputData = pOutput->columnData;

  int32_t outputLen = pInputData->varmeta.length;
  char   *outputBuf = taosMemoryCalloc(outputLen, 1);
  char   *output = outputBuf;

  for (int32_t i = 0; i < pInput->numOfRows; ++i) {
    if (colDataIsNull_s(pInputData, i)) {
      colDataSetNULL(pOutputData, i);
      continue;
    }

    char   *input = colDataGetData(pInputData, i);
    int32_t len = varDataLen(input);
    int32_t charLen = (type == TSDB_DATA_TYPE_VARCHAR) ? len : len / TSDB_NCHAR_SIZE;
    trimFn(input, output, type, charLen);

    colDataSetVal(pOutputData, i, output, false);
    output += varDataTLen(output);
  }

  pOutput->numOfRows = pInput->numOfRows;
  taosMemoryFree(outputBuf);

  return TSDB_CODE_SUCCESS;
}

int32_t substrFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  int32_t subPos = 0;
  GET_TYPED_DATA(subPos, int32_t, GET_PARAM_TYPE(&pInput[1]), pInput[1].columnData->pData);

  int32_t subLen = INT16_MAX;
  if (inputNum == 3) {
    GET_TYPED_DATA(subLen, int32_t, GET_PARAM_TYPE(&pInput[2]), pInput[2].columnData->pData);
    subLen = (GET_PARAM_TYPE(pInput) == TSDB_DATA_TYPE_VARCHAR) ? subLen : subLen * TSDB_NCHAR_SIZE;
  }

  SColumnInfoData *pInputData = pInput->columnData;
  SColumnInfoData *pOutputData = pOutput->columnData;

  int32_t outputLen = pInputData->info.bytes;
  char *outputBuf = taosMemoryMalloc(outputLen);
  if (outputBuf == NULL) {
    qError("substr function memory allocation failure. size: %d", outputLen);
    return TSDB_CODE_OUT_OF_MEMORY;
  }

  for (int32_t i = 0; i < pInput->numOfRows; ++i) {
    if (colDataIsNull_s(pInputData, i)) {
      colDataSetNULL(pOutputData, i);
      continue;
    }
    char   *input = colDataGetData(pInput[0].columnData, i);
    int32_t len = varDataLen(input);
    int32_t startPosBytes;

    if (subPos > 0) {
      startPosBytes = (GET_PARAM_TYPE(pInput) == TSDB_DATA_TYPE_VARCHAR) ? subPos - 1 : (subPos - 1) * TSDB_NCHAR_SIZE;
      startPosBytes = TMIN(startPosBytes, len);
    } else {
      startPosBytes =
          (GET_PARAM_TYPE(pInput) == TSDB_DATA_TYPE_VARCHAR) ? len + subPos : len + subPos * TSDB_NCHAR_SIZE;
      startPosBytes = TMAX(startPosBytes, 0);
    }

    char   *output = outputBuf;
    int32_t resLen = TMIN(subLen, len - startPosBytes);
    if (resLen > 0) {
      memcpy(varDataVal(output), varDataVal(input) + startPosBytes, resLen);
      varDataSetLen(output, resLen);
    } else {
      varDataSetLen(output, 0);
    }

    colDataSetVal(pOutputData, i, output, false);
  }

  pOutput->numOfRows = pInput->numOfRows;
  taosMemoryFree(outputBuf);

  return TSDB_CODE_SUCCESS;
}

/** Conversion functions **/
int32_t castFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  int16_t inputType = GET_PARAM_TYPE(&pInput[0]);
  int32_t inputLen = GET_PARAM_BYTES(&pInput[0]);
  int16_t outputType = GET_PARAM_TYPE(&pOutput[0]);
  int64_t outputLen = GET_PARAM_BYTES(&pOutput[0]);

  int32_t code = TSDB_CODE_SUCCESS;
  char   *convBuf = taosMemoryMalloc(inputLen);
  char   *output = taosMemoryCalloc(1, outputLen + TSDB_NCHAR_SIZE);
  char    buf[400] = {0};

  if (convBuf == NULL || output == NULL) {
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _end;
  }

  for (int32_t i = 0; i < pInput[0].numOfRows; ++i) {
    if (colDataIsNull_s(pInput[0].columnData, i)) {
      colDataSetNULL(pOutput->columnData, i);
      continue;
    }

    char *input = colDataGetData(pInput[0].columnData, i);

    switch (outputType) {
      case TSDB_DATA_TYPE_TINYINT: {
        if (inputType == TSDB_DATA_TYPE_BINARY) {
          memcpy(buf, varDataVal(input), varDataLen(input));
          buf[varDataLen(input)] = 0;
          *(int8_t *)output = taosStr2Int8(buf, NULL, 10);
        } else if (inputType == TSDB_DATA_TYPE_NCHAR) {
          int32_t len = taosUcs4ToMbs((TdUcs4 *)varDataVal(input), varDataLen(input), convBuf);
          if (len < 0) {
            code = TSDB_CODE_FAILED;
            goto _end;
          }

          convBuf[len] = 0;
          *(int8_t *)output = taosStr2Int8(convBuf, NULL, 10);
        } else {
          GET_TYPED_DATA(*(int8_t *)output, int8_t, inputType, input);
        }
        break;
      }
      case TSDB_DATA_TYPE_SMALLINT: {
        if (inputType == TSDB_DATA_TYPE_BINARY) {
          memcpy(buf, varDataVal(input), varDataLen(input));
          buf[varDataLen(input)] = 0;
          *(int16_t *)output = taosStr2Int16(buf, NULL, 10);
        } else if (inputType == TSDB_DATA_TYPE_NCHAR) {
          int32_t len = taosUcs4ToMbs((TdUcs4 *)varDataVal(input), varDataLen(input), convBuf);
          if (len < 0) {
            code = TSDB_CODE_FAILED;
            goto _end;
          }
          convBuf[len] = 0;
          *(int16_t *)output = taosStr2Int16(convBuf, NULL, 10);
        } else {
          GET_TYPED_DATA(*(int16_t *)output, int16_t, inputType, input);
        }
        break;
      }
      case TSDB_DATA_TYPE_INT: {
        if (inputType == TSDB_DATA_TYPE_BINARY) {
          memcpy(buf, varDataVal(input), varDataLen(input));
          buf[varDataLen(input)] = 0;
          *(int32_t *)output = taosStr2Int32(buf, NULL, 10);
        } else if (inputType == TSDB_DATA_TYPE_NCHAR) {
          int32_t len = taosUcs4ToMbs((TdUcs4 *)varDataVal(input), varDataLen(input), convBuf);
          if (len < 0) {
            code = TSDB_CODE_FAILED;
            goto _end;
          }

          convBuf[len] = 0;
          *(int32_t *)output = taosStr2Int32(convBuf, NULL, 10);
        } else {
          GET_TYPED_DATA(*(int32_t *)output, int32_t, inputType, input);
        }
        break;
      }
      case TSDB_DATA_TYPE_BIGINT: {
        if (inputType == TSDB_DATA_TYPE_BINARY) {
          memcpy(buf, varDataVal(input), varDataLen(input));
          buf[varDataLen(input)] = 0;
          *(int64_t *)output = taosStr2Int64(buf, NULL, 10);
        } else if (inputType == TSDB_DATA_TYPE_NCHAR) {
          int32_t len = taosUcs4ToMbs((TdUcs4 *)varDataVal(input), varDataLen(input), convBuf);
          if (len < 0) {
            code = TSDB_CODE_FAILED;
            goto _end;
          }
          convBuf[len] = 0;
          *(int64_t *)output = taosStr2Int64(convBuf, NULL, 10);
        } else {
          GET_TYPED_DATA(*(int64_t *)output, int64_t, inputType, input);
        }
        break;
      }
      case TSDB_DATA_TYPE_UTINYINT: {
        if (inputType == TSDB_DATA_TYPE_BINARY) {
          memcpy(buf, varDataVal(input), varDataLen(input));
          buf[varDataLen(input)] = 0;
          *(uint8_t *)output = taosStr2UInt8(buf, NULL, 10);
        } else if (inputType == TSDB_DATA_TYPE_NCHAR) {
          int32_t len = taosUcs4ToMbs((TdUcs4 *)varDataVal(input), varDataLen(input), convBuf);
          if (len < 0) {
            code = TSDB_CODE_FAILED;
            goto _end;
          }
          convBuf[len] = 0;
          *(uint8_t *)output = taosStr2UInt8(convBuf, NULL, 10);
        } else {
          GET_TYPED_DATA(*(uint8_t *)output, uint8_t, inputType, input);
        }
        break;
      }
      case TSDB_DATA_TYPE_USMALLINT: {
        if (inputType == TSDB_DATA_TYPE_BINARY) {
          memcpy(buf, varDataVal(input), varDataLen(input));
          buf[varDataLen(input)] = 0;
          *(uint16_t *)output = taosStr2UInt16(buf, NULL, 10);
        } else if (inputType == TSDB_DATA_TYPE_NCHAR) {
          int32_t len = taosUcs4ToMbs((TdUcs4 *)varDataVal(input), varDataLen(input), convBuf);
          if (len < 0) {
            code = TSDB_CODE_FAILED;
            goto _end;
          }
          convBuf[len] = 0;
          *(uint16_t *)output = taosStr2UInt16(convBuf, NULL, 10);
        } else {
          GET_TYPED_DATA(*(uint16_t *)output, uint16_t, inputType, input);
        }
        break;
      }
      case TSDB_DATA_TYPE_UINT: {
        if (inputType == TSDB_DATA_TYPE_BINARY) {
          memcpy(buf, varDataVal(input), varDataLen(input));
          buf[varDataLen(input)] = 0;
          *(uint32_t *)output = taosStr2UInt32(buf, NULL, 10);
        } else if (inputType == TSDB_DATA_TYPE_NCHAR) {
          int32_t len = taosUcs4ToMbs((TdUcs4 *)varDataVal(input), varDataLen(input), convBuf);
          if (len < 0) {
            code = TSDB_CODE_FAILED;
            goto _end;
          }
          convBuf[len] = 0;
          *(uint32_t *)output = taosStr2UInt32(convBuf, NULL, 10);
        } else {
          GET_TYPED_DATA(*(uint32_t *)output, uint32_t, inputType, input);
        }
        break;
      }
      case TSDB_DATA_TYPE_UBIGINT: {
        if (inputType == TSDB_DATA_TYPE_BINARY) {
          memcpy(buf, varDataVal(input), varDataLen(input));
          buf[varDataLen(input)] = 0;
          *(uint64_t *)output = taosStr2UInt64(buf, NULL, 10);
        } else if (inputType == TSDB_DATA_TYPE_NCHAR) {
          int32_t len = taosUcs4ToMbs((TdUcs4 *)varDataVal(input), varDataLen(input), convBuf);
          if (len < 0) {
            code = TSDB_CODE_FAILED;
            goto _end;
          }

          convBuf[len] = 0;
          *(uint64_t *)output = taosStr2UInt64(convBuf, NULL, 10);
        } else {
          GET_TYPED_DATA(*(uint64_t *)output, uint64_t, inputType, input);
        }
        break;
      }
      case TSDB_DATA_TYPE_FLOAT: {
        if (inputType == TSDB_DATA_TYPE_BINARY) {
          memcpy(buf, varDataVal(input), varDataLen(input));
          buf[varDataLen(input)] = 0;
          *(float *)output = taosStr2Float(buf, NULL);
        } else if (inputType == TSDB_DATA_TYPE_NCHAR) {
          int32_t len = taosUcs4ToMbs((TdUcs4 *)varDataVal(input), varDataLen(input), convBuf);
          if (len < 0) {
            code = TSDB_CODE_FAILED;
            goto _end;
          }
          convBuf[len] = 0;
          *(float *)output = taosStr2Float(convBuf, NULL);
        } else {
          GET_TYPED_DATA(*(float *)output, float, inputType, input);
        }
        break;
      }
      case TSDB_DATA_TYPE_DOUBLE: {
        if (inputType == TSDB_DATA_TYPE_BINARY) {
          memcpy(buf, varDataVal(input), varDataLen(input));
          buf[varDataLen(input)] = 0;
          *(double *)output = taosStr2Double(buf, NULL);
        } else if (inputType == TSDB_DATA_TYPE_NCHAR) {
          int32_t len = taosUcs4ToMbs((TdUcs4 *)varDataVal(input), varDataLen(input), convBuf);
          if (len < 0) {
            code = TSDB_CODE_FAILED;
            goto _end;
          }
          convBuf[len] = 0;
          *(double *)output = taosStr2Double(convBuf, NULL);
        } else {
          GET_TYPED_DATA(*(double *)output, double, inputType, input);
        }
        break;
      }
      case TSDB_DATA_TYPE_BOOL: {
        if (inputType == TSDB_DATA_TYPE_BINARY) {
          memcpy(buf, varDataVal(input), varDataLen(input));
          buf[varDataLen(input)] = 0;
          *(bool *)output = taosStr2Int8(buf, NULL, 10);
        } else if (inputType == TSDB_DATA_TYPE_NCHAR) {
          int32_t len = taosUcs4ToMbs((TdUcs4 *)varDataVal(input), varDataLen(input), convBuf);
          if (len < 0) {
            code = TSDB_CODE_FAILED;
            goto _end;
          }
          convBuf[len] = 0;
          *(bool *)output = taosStr2Int8(convBuf, NULL, 10);
        } else {
          GET_TYPED_DATA(*(bool *)output, bool, inputType, input);
        }
        break;
      }
      case TSDB_DATA_TYPE_TIMESTAMP: {
        int64_t timeVal;
        if (inputType == TSDB_DATA_TYPE_BINARY || inputType == TSDB_DATA_TYPE_NCHAR) {
          int64_t timePrec;
          GET_TYPED_DATA(timePrec, int64_t, GET_PARAM_TYPE(&pInput[1]), pInput[1].columnData->pData);
          int32_t ret = convertStringToTimestamp(inputType, input, timePrec, &timeVal);
          if (ret != TSDB_CODE_SUCCESS) {
            *(int64_t *)output = 0;
          } else {
            *(int64_t *)output = timeVal;
          }
        } else {
          GET_TYPED_DATA(*(int64_t *)output, int64_t, inputType, input);
        }
        break;
      }
      case TSDB_DATA_TYPE_BINARY:
      case TSDB_DATA_TYPE_GEOMETRY: {
        if (inputType == TSDB_DATA_TYPE_BOOL) {
          // NOTE: sprintf will append '\0' at the end of string
          int32_t len = sprintf(varDataVal(output), "%.*s", (int32_t)(outputLen - VARSTR_HEADER_SIZE),
                                *(int8_t *)input ? "true" : "false");
          varDataSetLen(output, len);
        } else if (inputType == TSDB_DATA_TYPE_BINARY) {
          int32_t len = TMIN(varDataLen(input), outputLen - VARSTR_HEADER_SIZE);
          memcpy(varDataVal(output), varDataVal(input), len);
          varDataSetLen(output, len);
        } else if (inputType == TSDB_DATA_TYPE_NCHAR) {
          int32_t len = taosUcs4ToMbs((TdUcs4 *)varDataVal(input), varDataLen(input), convBuf);
          if (len < 0) {
            code = TSDB_CODE_FAILED;
            goto _end;
          }
          len = TMIN(len, outputLen - VARSTR_HEADER_SIZE);
          memcpy(varDataVal(output), convBuf, len);
          varDataSetLen(output, len);
        } else {
          NUM_TO_STRING(inputType, input, sizeof(buf), buf);
          int32_t len = (int32_t)strlen(buf);
          len = (outputLen - VARSTR_HEADER_SIZE) > len ? len : (outputLen - VARSTR_HEADER_SIZE);
          memcpy(varDataVal(output), buf, len);
          varDataSetLen(output, len);
        }
        break;
      }
      case TSDB_DATA_TYPE_VARBINARY:{
        if (inputType == TSDB_DATA_TYPE_BINARY) {
          int32_t len = TMIN(varDataLen(input), outputLen - VARSTR_HEADER_SIZE);
          memcpy(varDataVal(output), varDataVal(input), len);
          varDataSetLen(output, len);
        }else{
          code = TSDB_CODE_FUNC_FUNTION_PARA_TYPE;
          goto _end;
        }
        break;
      }
      case TSDB_DATA_TYPE_NCHAR: {
        int32_t outputCharLen = (outputLen - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE;
        int32_t len;
        if (inputType == TSDB_DATA_TYPE_BOOL) {
          char tmp[8] = {0};
          len = sprintf(tmp, "%.*s", outputCharLen, *(int8_t *)input ? "true" : "false");
          bool ret = taosMbsToUcs4(tmp, len, (TdUcs4 *)varDataVal(output), outputLen - VARSTR_HEADER_SIZE, &len);
          if (!ret) {
            code = TSDB_CODE_FAILED;
            goto _end;
          }

          varDataSetLen(output, len);
        } else if (inputType == TSDB_DATA_TYPE_BINARY) {
          len = outputCharLen > varDataLen(input) ? varDataLen(input) : outputCharLen;
          bool ret = taosMbsToUcs4(input + VARSTR_HEADER_SIZE, len, (TdUcs4 *)varDataVal(output),
                                   outputLen - VARSTR_HEADER_SIZE, &len);
          if (!ret) {
            code = TSDB_CODE_FAILED;
            goto _end;
          }
          varDataSetLen(output, len);
        } else if (inputType == TSDB_DATA_TYPE_NCHAR) {
          len = TMIN(outputLen - VARSTR_HEADER_SIZE, varDataLen(input));
          memcpy(output, input, len + VARSTR_HEADER_SIZE);
          varDataSetLen(output, len);
        } else {
          NUM_TO_STRING(inputType, input, sizeof(buf), buf);
          len = (int32_t)strlen(buf);
          len = outputCharLen > len ? len : outputCharLen;
          bool ret = taosMbsToUcs4(buf, len, (TdUcs4 *)varDataVal(output), outputLen - VARSTR_HEADER_SIZE, &len);
          if (!ret) {
            code = TSDB_CODE_FAILED;
            goto _end;
          }
          varDataSetLen(output, len);
        }

        // for constant conversion, need to set proper length of pOutput description
        if (len < outputLen) {
          pOutput->columnData->info.bytes = len + VARSTR_HEADER_SIZE;
        }

        break;
      }
      default: {
        code = TSDB_CODE_FAILED;
        goto _end;
      }
    }

    colDataSetVal(pOutput->columnData, i, output, false);
  }

  pOutput->numOfRows = pInput->numOfRows;

_end:
  taosMemoryFree(output);
  taosMemoryFree(convBuf);
  return code;
}

int32_t toISO8601Function(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  int32_t type = GET_PARAM_TYPE(pInput);

  bool    tzPresent = (inputNum == 2) ? true : false;
  char    tz[20] = {0};
  int32_t tzLen = 0;
  if (tzPresent) {
    tzLen = varDataLen(pInput[1].columnData->pData);
    memcpy(tz, varDataVal(pInput[1].columnData->pData), tzLen);
  }

  for (int32_t i = 0; i < pInput[0].numOfRows; ++i) {
    if (colDataIsNull_s(pInput[0].columnData, i)) {
      colDataSetNULL(pOutput->columnData, i);
      continue;
    }

    char *input = colDataGetData(pInput[0].columnData, i);
    char  fraction[20] = {0};
    bool  hasFraction = false;
    NUM_TO_STRING(type, input, sizeof(fraction), fraction);
    int32_t tsDigits = (int32_t)strlen(fraction);

    char    buf[64] = {0};
    int64_t timeVal;
    GET_TYPED_DATA(timeVal, int64_t, type, input);
    if (tsDigits > TSDB_TIME_PRECISION_SEC_DIGITS) {
      if (tsDigits == TSDB_TIME_PRECISION_MILLI_DIGITS) {
        timeVal = timeVal / 1000;
      } else if (tsDigits == TSDB_TIME_PRECISION_MICRO_DIGITS) {
         timeVal = timeVal / ((int64_t)(1000 * 1000));
      } else if (tsDigits == TSDB_TIME_PRECISION_NANO_DIGITS) {
         timeVal = timeVal / ((int64_t)(1000 * 1000 * 1000));
      } else {
        colDataSetNULL(pOutput->columnData, i);
        continue;
      }
      hasFraction = true;
      memmove(fraction, fraction + TSDB_TIME_PRECISION_SEC_DIGITS, TSDB_TIME_PRECISION_SEC_DIGITS);
    }

    // trans current timezone's unix ts to dest timezone
    // offset = delta from dest timezone to zero
    // delta from zero to current timezone = 3600 * (cur)tsTimezone
    int64_t offset = 0;
    if (0 != offsetOfTimezone(tz, &offset)) {
      goto _end;
    }
    timeVal -= offset + 3600 * ((int64_t)tsTimezone);

    struct tm tmInfo;
    int32_t len = 0;

    if (taosLocalTime((const time_t *)&timeVal, &tmInfo, buf) == NULL) {
      len = (int32_t)strlen(buf);
      goto _end;
    }

    strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%S", &tmInfo);
    len = (int32_t)strlen(buf);

    // add timezone string
    if (tzLen > 0) {
      snprintf(buf + len, tzLen + 1, "%s", tz);
      len += tzLen;
    }

    if (hasFraction) {
      int32_t fracLen = (int32_t)strlen(fraction) + 1;

      char *tzInfo;
      if (buf[len - 1] == 'z' || buf[len - 1] == 'Z') {
        tzInfo = &buf[len - 1];
        memmove(tzInfo + fracLen, tzInfo, strlen(tzInfo));
      } else {
        tzInfo = strchr(buf, '+');
        if (tzInfo) {
          memmove(tzInfo + fracLen, tzInfo, strlen(tzInfo));
        } else {
          // search '-' backwards
          tzInfo = strrchr(buf, '-');
          if (tzInfo) {
            memmove(tzInfo + fracLen, tzInfo, strlen(tzInfo));
          }
        }
      }

      char tmp[32] = {0};
      sprintf(tmp, ".%s", fraction);
      memcpy(tzInfo, tmp, fracLen);
      len += fracLen;
    }

_end:
    memmove(buf + VARSTR_HEADER_SIZE, buf, len);
    varDataSetLen(buf, len);

    colDataSetVal(pOutput->columnData, i, buf, false);
  }

  pOutput->numOfRows = pInput->numOfRows;

  return TSDB_CODE_SUCCESS;
}

int32_t toUnixtimestampFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  int32_t type = GET_PARAM_TYPE(pInput);
  int64_t timePrec;
  int32_t idx = (inputNum == 2) ? 1 : 2;
  GET_TYPED_DATA(timePrec, int64_t, GET_PARAM_TYPE(&pInput[idx]), pInput[idx].columnData->pData);

  for (int32_t i = 0; i < pInput[0].numOfRows; ++i) {
    if (colDataIsNull_s(pInput[0].columnData, i)) {
      colDataSetNULL(pOutput->columnData, i);
      continue;
    }
    char *input = colDataGetData(pInput[0].columnData, i);

    int64_t timeVal = 0;
    int32_t ret = convertStringToTimestamp(type, input, timePrec, &timeVal);
    if (ret != TSDB_CODE_SUCCESS) {
      colDataSetNULL(pOutput->columnData, i);
    } else {
      colDataSetVal(pOutput->columnData, i, (char *)&timeVal, false);
    }
  }

  pOutput->numOfRows = pInput->numOfRows;

  return TSDB_CODE_SUCCESS;
}

int32_t toJsonFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  int32_t type = GET_PARAM_TYPE(pInput);

  char tmp[TSDB_MAX_JSON_TAG_LEN] = {0};
  for (int32_t i = 0; i < pInput[0].numOfRows; ++i) {
    SArray *pTagVals = taosArrayInit(8, sizeof(STagVal));
    STag   *pTag = NULL;

    if (colDataIsNull_s(pInput[0].columnData, i)) {
      tTagNew(pTagVals, 1, true, &pTag);
    } else {
      char *input = pInput[0].columnData->pData + pInput[0].columnData->varmeta.offset[i];
      if (varDataLen(input) > (TSDB_MAX_JSON_TAG_LEN - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE) {
        taosArrayDestroy(pTagVals);
        return TSDB_CODE_FAILED;
      }
      memcpy(tmp, varDataVal(input), varDataLen(input));
      tmp[varDataLen(input)] = 0;
      if (parseJsontoTagData(tmp, pTagVals, &pTag, NULL)) {
        tTagNew(pTagVals, 1, true, &pTag);
      }
    }

    colDataSetVal(pOutput->columnData, i, (const char *)pTag, false);
    tTagFree(pTag);
    taosArrayDestroy(pTagVals);
  }

  pOutput->numOfRows = pInput->numOfRows;
  return TSDB_CODE_SUCCESS;
}

#define TS_FORMAT_MAX_LEN 4096
int32_t toTimestampFunction(SScalarParam* pInput, int32_t inputNum, SScalarParam* pOutput) {
  int64_t ts;
  char *  tsStr = taosMemoryMalloc(TS_FORMAT_MAX_LEN);
  char *  format = taosMemoryMalloc(TS_FORMAT_MAX_LEN);
  int32_t len, code = TSDB_CODE_SUCCESS;
  SArray *formats = NULL;

  for (int32_t i = 0; i < pInput[0].numOfRows; ++i) {
    if (colDataIsNull_s(pInput[1].columnData, i) || colDataIsNull_s(pInput[0].columnData, i)) {
      colDataSetNULL(pOutput->columnData, i);
      continue;
    }

    char *tsData = colDataGetData(pInput[0].columnData, i);
    char *formatData = colDataGetData(pInput[1].columnData, pInput[1].numOfRows > 1 ? i : 0);
    len = TMIN(TS_FORMAT_MAX_LEN - 1, varDataLen(tsData));
    strncpy(tsStr, varDataVal(tsData), len);
    tsStr[len] = '\0';
    len = TMIN(TS_FORMAT_MAX_LEN - 1, varDataLen(formatData));
    if (pInput[1].numOfRows > 1 || i == 0) {
      strncpy(format, varDataVal(formatData), len);
      format[len] = '\0';
      if (formats) {
        taosArrayDestroy(formats);
        formats = NULL;
      }
    }
    int32_t precision = pOutput->columnData->info.precision;
    char    errMsg[128] = {0};
    code = taosChar2Ts(format, &formats, tsStr, &ts, precision, errMsg, 128);
    if (code) {
      qError("func to_timestamp failed %s", errMsg);
      break;
    }
    colDataSetVal(pOutput->columnData, i, (char *)&ts, false);
  }
  if (formats) taosArrayDestroy(formats);
  taosMemoryFree(tsStr);
  taosMemoryFree(format);
  return code;
}

int32_t toCharFunction(SScalarParam* pInput, int32_t inputNum, SScalarParam* pOutput) {
  char *  format = taosMemoryMalloc(TS_FORMAT_MAX_LEN);
  char *  out = taosMemoryCalloc(1, TS_FORMAT_MAX_LEN + VARSTR_HEADER_SIZE);
  int32_t len;
  SArray *formats = NULL;
  int32_t code = 0;
  for (int32_t i = 0; i < pInput[0].numOfRows; ++i) {
    if (colDataIsNull_s(pInput[1].columnData, i) || colDataIsNull_s(pInput[0].columnData, i)) {
      colDataSetNULL(pOutput->columnData, i);
      continue;
    }

    char *ts = colDataGetData(pInput[0].columnData, i);
    char *formatData = colDataGetData(pInput[1].columnData, pInput[1].numOfRows > 1 ? i : 0);
    len = TMIN(TS_FORMAT_MAX_LEN - 1, varDataLen(formatData));
    if (pInput[1].numOfRows > 1 || i == 0) {
      strncpy(format, varDataVal(formatData), len);
      format[len] = '\0';
      if (formats) {
        taosArrayDestroy(formats);
        formats = NULL;
      }
    }
    int32_t precision = pInput[0].columnData->info.precision;
    code = taosTs2Char(format, &formats, *(int64_t *)ts, precision, varDataVal(out), TS_FORMAT_MAX_LEN);
    if (code) break;
    varDataSetLen(out, strlen(varDataVal(out)));
    colDataSetVal(pOutput->columnData, i, out, false);
  }
  if (formats) taosArrayDestroy(formats);
  taosMemoryFree(format);
  taosMemoryFree(out);
  return code;
}

/** Time functions **/
static int64_t offsetFromTz(char *timezone, int64_t factor) {
  char *minStr = &timezone[3];
  int64_t minutes = taosStr2Int64(minStr, NULL, 10);
  memset(minStr, 0, strlen(minStr));
  int64_t hours = taosStr2Int64(timezone, NULL, 10);
  int64_t seconds = hours * 3600 + minutes * 60;

  return seconds * factor;

}

int32_t timeTruncateFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  int32_t type = GET_PARAM_TYPE(&pInput[0]);

  int64_t timeUnit, timePrec, timeVal = 0;
  bool    ignoreTz = true;
  char    timezone[20] = {0};

  GET_TYPED_DATA(timeUnit, int64_t, GET_PARAM_TYPE(&pInput[1]), pInput[1].columnData->pData);

  int32_t timePrecIdx = 2, timeZoneIdx = 3;
  if (inputNum == 5) {
    timePrecIdx += 1;
    timeZoneIdx += 1;
    GET_TYPED_DATA(ignoreTz, bool, GET_PARAM_TYPE(&pInput[2]), pInput[2].columnData->pData);
  }

  GET_TYPED_DATA(timePrec, int64_t, GET_PARAM_TYPE(&pInput[timePrecIdx]), pInput[timePrecIdx].columnData->pData);
  memcpy(timezone, varDataVal(pInput[timeZoneIdx].columnData->pData), varDataLen(pInput[timeZoneIdx].columnData->pData));

  int64_t factor = TSDB_TICK_PER_SECOND(timePrec);
  int64_t unit = timeUnit * 1000 / factor;

  for (int32_t i = 0; i < pInput[0].numOfRows; ++i) {
    if (colDataIsNull_s(pInput[0].columnData, i)) {
      colDataSetNULL(pOutput->columnData, i);
      continue;
    }

    char *input = colDataGetData(pInput[0].columnData, i);

    if (IS_VAR_DATA_TYPE(type)) { /* datetime format strings */
      int32_t ret = convertStringToTimestamp(type, input, TSDB_TIME_PRECISION_NANO, &timeVal);
      if (ret != TSDB_CODE_SUCCESS) {
        colDataSetNULL(pOutput->columnData, i);
        continue;
      }
      // If converted value is less than 10digits in second, use value in second instead
      int64_t timeValSec = timeVal / 1000000000;
      if (timeValSec < 1000000000) {
        timeVal = timeValSec;
      }
    } else if (type == TSDB_DATA_TYPE_BIGINT) { /* unix timestamp */
      GET_TYPED_DATA(timeVal, int64_t, type, input);
    } else if (type == TSDB_DATA_TYPE_TIMESTAMP) { /* timestamp column*/
      GET_TYPED_DATA(timeVal, int64_t, type, input);
      int64_t timeValSec = timeVal / factor;
      if (timeValSec < 1000000000) {
        timeVal = timeValSec;
      }
    }

    char buf[20] = {0};
    NUM_TO_STRING(TSDB_DATA_TYPE_BIGINT, &timeVal, sizeof(buf), buf);
    int32_t tsDigits = (int32_t)strlen(buf);

    switch (unit) {
      case 0: { /* 1u or 1b */
        if (tsDigits == TSDB_TIME_PRECISION_NANO_DIGITS) {
          if (timePrec == TSDB_TIME_PRECISION_NANO && timeUnit == 1) {
            timeVal = timeVal * 1;
          } else {
            timeVal = timeVal / 1000 * 1000;
          }
        } else if (tsDigits <= TSDB_TIME_PRECISION_SEC_DIGITS) {
          timeVal = timeVal * factor;
        } else {
          timeVal = timeVal * 1;
        }
        break;
      }
      case 1: { /* 1a */
        if (tsDigits == TSDB_TIME_PRECISION_MILLI_DIGITS) {
          timeVal = timeVal * 1;
        } else if (tsDigits == TSDB_TIME_PRECISION_MICRO_DIGITS) {
          timeVal = timeVal / 1000 * 1000;
        } else if (tsDigits == TSDB_TIME_PRECISION_NANO_DIGITS) {
          timeVal = timeVal / 1000000 * 1000000;
        } else if (tsDigits <= TSDB_TIME_PRECISION_SEC_DIGITS) {
          timeVal = timeVal * factor;
        } else {
          colDataSetNULL(pOutput->columnData, i);
          continue;
        }
        break;
      }
      case 1000: { /* 1s */
        if (tsDigits == TSDB_TIME_PRECISION_MILLI_DIGITS) {
          timeVal = timeVal / 1000 * 1000;
        } else if (tsDigits == TSDB_TIME_PRECISION_MICRO_DIGITS) {
          timeVal = timeVal / 1000000 * 1000000;
        } else if (tsDigits == TSDB_TIME_PRECISION_NANO_DIGITS) {
          timeVal = timeVal / 1000000000 * 1000000000;
        } else if (tsDigits <= TSDB_TIME_PRECISION_SEC_DIGITS) {
          timeVal = timeVal * factor;
        } else {
          colDataSetNULL(pOutput->columnData, i);
          continue;
        }
        break;
      }
      case 60000: { /* 1m */
        if (tsDigits == TSDB_TIME_PRECISION_MILLI_DIGITS) {
          timeVal = timeVal / 1000 / 60 * 60 * 1000;
        } else if (tsDigits == TSDB_TIME_PRECISION_MICRO_DIGITS) {
          timeVal = timeVal / 1000000 / 60 * 60 * 1000000;
        } else if (tsDigits == TSDB_TIME_PRECISION_NANO_DIGITS) {
          timeVal = timeVal / 1000000000 / 60 * 60 * 1000000000;
        } else if (tsDigits <= TSDB_TIME_PRECISION_SEC_DIGITS) {
          timeVal = timeVal * factor / factor / 60 * 60 * factor;
        } else {
          colDataSetNULL(pOutput->columnData, i);
          continue;
        }
        break;
      }
      case 3600000: { /* 1h */
        if (tsDigits == TSDB_TIME_PRECISION_MILLI_DIGITS) {
          timeVal = timeVal / 1000 / 3600 * 3600 * 1000;
        } else if (tsDigits == TSDB_TIME_PRECISION_MICRO_DIGITS) {
          timeVal = timeVal / 1000000 / 3600 * 3600 * 1000000;
        } else if (tsDigits == TSDB_TIME_PRECISION_NANO_DIGITS) {
          timeVal = timeVal / 1000000000 / 3600 * 3600 * 1000000000;
        } else if (tsDigits <= TSDB_TIME_PRECISION_SEC_DIGITS) {
          timeVal = timeVal * factor / factor / 3600 * 3600 * factor;
        } else {
          colDataSetNULL(pOutput->columnData, i);
          continue;
        }
        break;
      }
      case 86400000: { /* 1d */
        if (tsDigits == TSDB_TIME_PRECISION_MILLI_DIGITS) {
          if (ignoreTz) {
            timeVal = timeVal - (timeVal + offsetFromTz(timezone, 1000)) % (((int64_t)86400) * 1000);
          } else {
            timeVal = timeVal / 1000 / 86400 * 86400 * 1000;
          }
        } else if (tsDigits == TSDB_TIME_PRECISION_MICRO_DIGITS) {
          if (ignoreTz) {
            timeVal = timeVal - (timeVal + offsetFromTz(timezone, 1000000)) % (((int64_t)86400) * 1000000);
          } else {
            timeVal = timeVal / 1000000 / 86400 * 86400 * 1000000;
          }
        } else if (tsDigits == TSDB_TIME_PRECISION_NANO_DIGITS) {
          if (ignoreTz) {
            timeVal = timeVal - (timeVal + offsetFromTz(timezone, 1000000000)) % (((int64_t)86400) * 1000000000);
          } else {
            timeVal = timeVal / 1000000000 / 86400 * 86400 * 1000000000;
          }
        } else if (tsDigits <= TSDB_TIME_PRECISION_SEC_DIGITS) {
          if (ignoreTz) {
            timeVal = (timeVal - (timeVal + offsetFromTz(timezone, 1)) % (86400L)) * factor;
          } else {
            timeVal = timeVal * factor / factor / 86400 * 86400 * factor;
          }
        } else {
          colDataSetNULL(pOutput->columnData, i);
          continue;
        }
        break;
      }
      case 604800000: { /* 1w */
        if (tsDigits == TSDB_TIME_PRECISION_MILLI_DIGITS) {
          if (ignoreTz) {
            timeVal = timeVal - (timeVal + offsetFromTz(timezone, 1000)) % (((int64_t)604800) * 1000);
          } else {
            timeVal = timeVal / 1000 / 604800 * 604800 * 1000;
          }
        } else if (tsDigits == TSDB_TIME_PRECISION_MICRO_DIGITS) {
          if (ignoreTz) {
            timeVal = timeVal - (timeVal + offsetFromTz(timezone, 1000000)) % (((int64_t)604800) * 1000000);
          } else {
            timeVal = timeVal / 1000000 / 604800 * 604800 * 1000000;
          }
        } else if (tsDigits == TSDB_TIME_PRECISION_NANO_DIGITS) {
          if (ignoreTz) {
            timeVal = timeVal - (timeVal + offsetFromTz(timezone, 1000000000)) % (((int64_t)604800) * 1000000000);
          } else {
            timeVal = timeVal / 1000000000 / 604800 * 604800 * 1000000000;
          }
        } else if (tsDigits <= TSDB_TIME_PRECISION_SEC_DIGITS) {
          if (ignoreTz) {
            timeVal = timeVal - (timeVal + offsetFromTz(timezone, 1)) % (((int64_t)604800L) * factor);
          } else {
            timeVal = timeVal * factor / factor / 604800 * 604800 * factor;
          }
        } else {
          colDataSetNULL(pOutput->columnData, i);
          continue;
        }
        break;
      }
      default: {
        timeVal = timeVal * 1;
        break;
      }
    }

    // truncate the timestamp to db precision
    switch (timePrec) {
      case TSDB_TIME_PRECISION_MILLI: {
        if (tsDigits == TSDB_TIME_PRECISION_MICRO_DIGITS) {
          timeVal = timeVal / 1000;
        } else if (tsDigits == TSDB_TIME_PRECISION_NANO_DIGITS) {
          timeVal = timeVal / 1000000;
        }
        break;
      }
      case TSDB_TIME_PRECISION_MICRO: {
        if (tsDigits == TSDB_TIME_PRECISION_NANO_DIGITS) {
          timeVal = timeVal / 1000;
        } else if (tsDigits == TSDB_TIME_PRECISION_MILLI_DIGITS) {
          timeVal = timeVal * 1000;
        }
        break;
      }
      case TSDB_TIME_PRECISION_NANO: {
        if (tsDigits == TSDB_TIME_PRECISION_MICRO_DIGITS) {
          timeVal = timeVal * 1000;
        } else if (tsDigits == TSDB_TIME_PRECISION_MILLI_DIGITS) {
          timeVal = timeVal * 1000000;
        }
        break;
      }
    }

    colDataSetVal(pOutput->columnData, i, (char *)&timeVal, false);
  }

  pOutput->numOfRows = pInput->numOfRows;

  return TSDB_CODE_SUCCESS;
}

int32_t timeDiffFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  int64_t timeUnit = -1, timePrec, timeVal[2] = {0};
  if (inputNum == 4) {
    GET_TYPED_DATA(timeUnit, int64_t, GET_PARAM_TYPE(&pInput[2]), pInput[2].columnData->pData);
    GET_TYPED_DATA(timePrec, int64_t, GET_PARAM_TYPE(&pInput[3]), pInput[3].columnData->pData);
  } else {
    GET_TYPED_DATA(timePrec, int64_t, GET_PARAM_TYPE(&pInput[2]), pInput[2].columnData->pData);
  }

  int64_t factor = TSDB_TICK_PER_SECOND(timePrec);
  int32_t numOfRows = 0;
  for (int32_t i = 0; i < inputNum; ++i) {
    if (pInput[i].numOfRows > numOfRows) {
      numOfRows = pInput[i].numOfRows;
    }
  }

  char *input[2];
  for (int32_t i = 0; i < numOfRows; ++i) {
    bool hasNull = false;
    for (int32_t k = 0; k < 2; ++k) {
      if (colDataIsNull_s(pInput[k].columnData, i)) {
        hasNull = true;
        break;
      }

      int32_t rowIdx = (pInput[k].numOfRows == 1) ? 0 : i;
      input[k] = colDataGetData(pInput[k].columnData, rowIdx);

      int32_t type = GET_PARAM_TYPE(&pInput[k]);
      if (IS_VAR_DATA_TYPE(type)) { /* datetime format strings */
        int32_t ret = convertStringToTimestamp(type, input[k], TSDB_TIME_PRECISION_NANO, &timeVal[k]);
        if (ret != TSDB_CODE_SUCCESS) {
          hasNull = true;
          break;
        }
      } else if (type == TSDB_DATA_TYPE_BIGINT || type == TSDB_DATA_TYPE_TIMESTAMP) { /* unix timestamp or ts column*/
        GET_TYPED_DATA(timeVal[k], int64_t, type, input[k]);
        if (type == TSDB_DATA_TYPE_TIMESTAMP) {
          int64_t timeValSec = timeVal[k] / factor;
          if (timeValSec < 1000000000) {
            timeVal[k] = timeValSec;
          }
        }

        char buf[20] = {0};
        NUM_TO_STRING(TSDB_DATA_TYPE_BIGINT, &timeVal[k], sizeof(buf), buf);
        int32_t tsDigits = (int32_t)strlen(buf);
        if (tsDigits <= TSDB_TIME_PRECISION_SEC_DIGITS) {
          timeVal[k] = timeVal[k] * 1000000000;
        } else if (tsDigits == TSDB_TIME_PRECISION_MILLI_DIGITS) {
          timeVal[k] = timeVal[k] * 1000000;
        } else if (tsDigits == TSDB_TIME_PRECISION_MICRO_DIGITS) {
          timeVal[k] = timeVal[k] * 1000;
        } else if (tsDigits == TSDB_TIME_PRECISION_NANO_DIGITS) {
          timeVal[k] = timeVal[k] * 1;
        } else {
          hasNull = true;
          break;
        }
      }
    }

    if (hasNull) {
      colDataSetNULL(pOutput->columnData, i);
      continue;
    }

    int64_t result = (timeVal[0] >= timeVal[1]) ? (timeVal[0] - timeVal[1]) : (timeVal[1] - timeVal[0]);

    if (timeUnit < 0) {  // if no time unit given use db precision
      switch (timePrec) {
        case TSDB_TIME_PRECISION_MILLI: {
          result = result / 1000000;
          break;
        }
        case TSDB_TIME_PRECISION_MICRO: {
          result = result / 1000;
          break;
        }
        case TSDB_TIME_PRECISION_NANO: {
          result = result / 1;
          break;
        }
      }
    } else {
      int64_t unit = timeUnit * 1000 / factor;
      switch (unit) {
        case 0: { /* 1u or 1b */
          if (timePrec == TSDB_TIME_PRECISION_NANO && timeUnit == 1) {
            result = result / 1;
          } else {
            result = result / 1000;
          }
          break;
        }
        case 1: { /* 1a */
          result = result / 1000000;
          break;
        }
        case 1000: { /* 1s */
          result = result / 1000000000;
          break;
        }
        case 60000: { /* 1m */
          result = result / 1000000000 / 60;
          break;
        }
        case 3600000: { /* 1h */
          result = result / 1000000000 / 3600;
          break;
        }
        case 86400000: { /* 1d */
          result = result / 1000000000 / 86400;
          break;
        }
        case 604800000: { /* 1w */
          result = result / 1000000000 / 604800;
          break;
        }
        default: {
          break;
        }
      }
    }

    colDataSetVal(pOutput->columnData, i, (char *)&result, false);
  }

  pOutput->numOfRows = numOfRows;

  return TSDB_CODE_SUCCESS;
}

int32_t nowFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  int64_t timePrec;
  GET_TYPED_DATA(timePrec, int64_t, GET_PARAM_TYPE(&pInput[0]), pInput[0].columnData->pData);

  int64_t ts = taosGetTimestamp(timePrec);
  for (int32_t i = 0; i < pInput->numOfRows; ++i) {
    colDataSetInt64(pOutput->columnData, i, &ts);
  }
  pOutput->numOfRows = pInput->numOfRows;
  return TSDB_CODE_SUCCESS;
}

int32_t todayFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  int64_t timePrec;
  GET_TYPED_DATA(timePrec, int64_t, GET_PARAM_TYPE(&pInput[0]), pInput[0].columnData->pData);

  int64_t ts = taosGetTimestampToday(timePrec);
  for (int32_t i = 0; i < pInput->numOfRows; ++i) {
    colDataSetInt64(pOutput->columnData, i, &ts);
  }
  pOutput->numOfRows = pInput->numOfRows;
  return TSDB_CODE_SUCCESS;
}

int32_t timezoneFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  char output[TD_TIMEZONE_LEN + VARSTR_HEADER_SIZE] = {0};
  memcpy(varDataVal(output), tsTimezoneStr, TD_TIMEZONE_LEN);
  varDataSetLen(output, strlen(tsTimezoneStr));
  for (int32_t i = 0; i < pInput->numOfRows; ++i) {
    colDataSetVal(pOutput->columnData, i, output, false);
  }
  pOutput->numOfRows = pInput->numOfRows;
  return TSDB_CODE_SUCCESS;
}

int32_t atanFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  return doScalarFunctionUnique(pInput, inputNum, pOutput, atan);
}

int32_t sinFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  return doScalarFunctionUnique(pInput, inputNum, pOutput, sin);
}

int32_t cosFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  return doScalarFunctionUnique(pInput, inputNum, pOutput, cos);
}

int32_t tanFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  return doScalarFunctionUnique(pInput, inputNum, pOutput, tan);
}

int32_t asinFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  return doScalarFunctionUnique(pInput, inputNum, pOutput, asin);
}

int32_t acosFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  return doScalarFunctionUnique(pInput, inputNum, pOutput, acos);
}

int32_t powFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  return doScalarFunctionUnique2(pInput, inputNum, pOutput, pow);
}

int32_t logFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  if (inputNum == 1) {
    return doScalarFunctionUnique(pInput, inputNum, pOutput, tlog);
  } else {
    return doScalarFunctionUnique2(pInput, inputNum, pOutput, tlog2);
  }
}

int32_t sqrtFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  return doScalarFunctionUnique(pInput, inputNum, pOutput, sqrt);
}

int32_t ceilFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  return doScalarFunction(pInput, inputNum, pOutput, ceilf, ceil);
}

int32_t floorFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  return doScalarFunction(pInput, inputNum, pOutput, floorf, floor);
}

int32_t roundFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  return doScalarFunction(pInput, inputNum, pOutput, roundf, round);
}

int32_t lowerFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
#ifdef WINDOWS
  return doCaseConvFunction(pInput, inputNum, pOutput, towlower);
#else
  return doCaseConvFunction(pInput, inputNum, pOutput, tolower);
#endif
}

int32_t upperFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
#ifdef WINDOWS
  return doCaseConvFunction(pInput, inputNum, pOutput, towupper);
#else
  return doCaseConvFunction(pInput, inputNum, pOutput, toupper);
#endif
}

int32_t ltrimFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  return doTrimFunction(pInput, inputNum, pOutput, tltrim);
}

int32_t rtrimFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  return doTrimFunction(pInput, inputNum, pOutput, trtrim);
}

int32_t lengthFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  return doLengthFunction(pInput, inputNum, pOutput, tlength);
}

int32_t charLengthFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  return doLengthFunction(pInput, inputNum, pOutput, tcharlength);
}

bool getTimePseudoFuncEnv(SFunctionNode *UNUSED_PARAM(pFunc), SFuncExecEnv *pEnv) {
  pEnv->calcMemSize = sizeof(int64_t);
  return true;
}

#ifdef BUILD_NO_CALL
int32_t qStartTsFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  colDataSetInt64(pOutput->columnData, pOutput->numOfRows, (int64_t *)colDataGetData(pInput->columnData, 0));
  return TSDB_CODE_SUCCESS;
}

int32_t qEndTsFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  colDataSetInt64(pOutput->columnData, pOutput->numOfRows, (int64_t *)colDataGetData(pInput->columnData, 1));
  return TSDB_CODE_SUCCESS;
}
#endif

int32_t winDurFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  colDataSetInt64(pOutput->columnData, pOutput->numOfRows, (int64_t *)colDataGetData(pInput->columnData, 2));
  return TSDB_CODE_SUCCESS;
}

int32_t winStartTsFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  colDataSetInt64(pOutput->columnData, pOutput->numOfRows, (int64_t *)colDataGetData(pInput->columnData, 3));
  return TSDB_CODE_SUCCESS;
}

int32_t winEndTsFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  colDataSetInt64(pOutput->columnData, pOutput->numOfRows, (int64_t *)colDataGetData(pInput->columnData, 4));
  return TSDB_CODE_SUCCESS;
}

int32_t qPseudoTagFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  char   *p = colDataGetData(pInput->columnData, 0);
  int32_t code = colDataSetNItems(pOutput->columnData, pOutput->numOfRows, p, pInput->numOfRows, true);
  if (code) {
    return code;
  }
  
  pOutput->numOfRows += pInput->numOfRows;
  return TSDB_CODE_SUCCESS;
}

/** Aggregation functions **/
int32_t countScalarFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  SColumnInfoData *pInputData = pInput->columnData;
  SColumnInfoData *pOutputData = pOutput->columnData;

  int64_t *out = (int64_t *)pOutputData->pData;
  *out = 0;
  for (int32_t i = 0; i < pInput->numOfRows; ++i) {
    if (colDataIsNull_s(pInputData, i)) {
      continue;
    }
    (*out)++;
  }

  pOutput->numOfRows = 1;
  return TSDB_CODE_SUCCESS;
}

int32_t sumScalarFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  SColumnInfoData *pInputData = pInput->columnData;
  SColumnInfoData *pOutputData = pOutput->columnData;

  int32_t type = GET_PARAM_TYPE(pInput);
  bool    hasNull = false;
  for (int32_t i = 0; i < pInput->numOfRows; ++i) {
    if (colDataIsNull_s(pInputData, i)) {
      hasNull = true;
      break;
    }

    if (IS_SIGNED_NUMERIC_TYPE(type) || type == TSDB_DATA_TYPE_BOOL) {
      int64_t *out = (int64_t *)pOutputData->pData;
      if (type == TSDB_DATA_TYPE_TINYINT || type == TSDB_DATA_TYPE_BOOL) {
        int8_t *in = (int8_t *)pInputData->pData;
        *out += in[i];
      } else if (type == TSDB_DATA_TYPE_SMALLINT) {
        int16_t *in = (int16_t *)pInputData->pData;
        *out += in[i];
      } else if (type == TSDB_DATA_TYPE_INT) {
        int32_t *in = (int32_t *)pInputData->pData;
        *out += in[i];
      } else if (type == TSDB_DATA_TYPE_BIGINT) {
        int64_t *in = (int64_t *)pInputData->pData;
        *out += in[i];
      }
    } else if (IS_UNSIGNED_NUMERIC_TYPE(type)) {
      uint64_t *out = (uint64_t *)pOutputData->pData;
      if (type == TSDB_DATA_TYPE_UTINYINT) {
        uint8_t *in = (uint8_t *)pInputData->pData;
        *out += in[i];
      } else if (type == TSDB_DATA_TYPE_USMALLINT) {
        uint16_t *in = (uint16_t *)pInputData->pData;
        *out += in[i];
      } else if (type == TSDB_DATA_TYPE_UINT) {
        uint32_t *in = (uint32_t *)pInputData->pData;
        *out += in[i];
      } else if (type == TSDB_DATA_TYPE_UBIGINT) {
        uint64_t *in = (uint64_t *)pInputData->pData;
        *out += in[i];
      }
    } else if (IS_FLOAT_TYPE(type)) {
      double *out = (double *)pOutputData->pData;
      if (type == TSDB_DATA_TYPE_FLOAT) {
        float *in = (float *)pInputData->pData;
        *out += in[i];
      } else if (type == TSDB_DATA_TYPE_DOUBLE) {
        double *in = (double *)pInputData->pData;
        *out += in[i];
      }
    }
  }

  if (hasNull) {
    colDataSetNULL(pOutputData, 0);
  }

  pOutput->numOfRows = 1;
  return TSDB_CODE_SUCCESS;
}

static int32_t doMinMaxScalarFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput, bool isMinFunc) {
  SColumnInfoData *pInputData = pInput->columnData;
  SColumnInfoData *pOutputData = pOutput->columnData;

  int32_t type = GET_PARAM_TYPE(pInput);

  bool hasNull = false;
  if (isMinFunc) {
    SET_TYPED_DATA_MAX(pOutputData->pData, type);
  } else {
    SET_TYPED_DATA_MIN(pOutputData->pData, type);
  }

  for (int32_t i = 0; i < pInput->numOfRows; ++i) {
    if (colDataIsNull_s(pInputData, i)) {
      hasNull = true;
      break;
    }

    switch (type) {
      case TSDB_DATA_TYPE_BOOL:
      case TSDB_DATA_TYPE_TINYINT: {
        int8_t *in = (int8_t *)pInputData->pData;
        int8_t *out = (int8_t *)pOutputData->pData;
        if ((in[i] > *out) ^ isMinFunc) {
          *out = in[i];
        }
        break;
      }
      case TSDB_DATA_TYPE_SMALLINT: {
        int16_t *in = (int16_t *)pInputData->pData;
        int16_t *out = (int16_t *)pOutputData->pData;
        if ((in[i] > *out) ^ isMinFunc) {
          *out = in[i];
        }
        break;
      }
      case TSDB_DATA_TYPE_INT: {
        int32_t *in = (int32_t *)pInputData->pData;
        int32_t *out = (int32_t *)pOutputData->pData;
        if ((in[i] > *out) ^ isMinFunc) {
          *out = in[i];
        }
        break;
      }
      case TSDB_DATA_TYPE_BIGINT: {
        int64_t *in = (int64_t *)pInputData->pData;
        int64_t *out = (int64_t *)pOutputData->pData;
        if ((in[i] > *out) ^ isMinFunc) {
          *out = in[i];
        }
        break;
      }
      case TSDB_DATA_TYPE_UTINYINT: {
        uint8_t *in = (uint8_t *)pInputData->pData;
        uint8_t *out = (uint8_t *)pOutputData->pData;
        if ((in[i] > *out) ^ isMinFunc) {
          *out = in[i];
        }
        break;
      }
      case TSDB_DATA_TYPE_USMALLINT: {
        uint16_t *in = (uint16_t *)pInputData->pData;
        uint16_t *out = (uint16_t *)pOutputData->pData;
        if ((in[i] > *out) ^ isMinFunc) {
          *out = in[i];
        }
        break;
      }
      case TSDB_DATA_TYPE_UINT: {
        uint32_t *in = (uint32_t *)pInputData->pData;
        uint32_t *out = (uint32_t *)pOutputData->pData;
        if ((in[i] > *out) ^ isMinFunc) {
          *out = in[i];
        }
        break;
      }
      case TSDB_DATA_TYPE_UBIGINT: {
        uint64_t *in = (uint64_t *)pInputData->pData;
        uint64_t *out = (uint64_t *)pOutputData->pData;
        if ((in[i] > *out) ^ isMinFunc) {
          *out = in[i];
        }
        break;
      }
      case TSDB_DATA_TYPE_FLOAT: {
        float *in = (float *)pInputData->pData;
        float *out = (float *)pOutputData->pData;
        if ((in[i] > *out) ^ isMinFunc) {
          *out = in[i];
        }
        break;
      }
      case TSDB_DATA_TYPE_DOUBLE: {
        double *in = (double *)pInputData->pData;
        double *out = (double *)pOutputData->pData;
        if ((in[i] > *out) ^ isMinFunc) {
          *out = in[i];
        }
        break;
      }
    }
  }

  if (hasNull) {
    colDataSetNULL(pOutputData, 0);
  }

  pOutput->numOfRows = 1;
  return TSDB_CODE_SUCCESS;
}

int32_t minScalarFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  return doMinMaxScalarFunction(pInput, inputNum, pOutput, true);
}

int32_t maxScalarFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  return doMinMaxScalarFunction(pInput, inputNum, pOutput, false);
}

int32_t avgScalarFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  SColumnInfoData *pInputData = pInput->columnData;
  SColumnInfoData *pOutputData = pOutput->columnData;

  int32_t type = GET_PARAM_TYPE(pInput);
  int64_t count = 0;
  bool    hasNull = false;

  for (int32_t i = 0; i < pInput->numOfRows; ++i) {
    if (colDataIsNull_s(pInputData, i)) {
      hasNull = true;
      break;
    }

    switch (type) {
      case TSDB_DATA_TYPE_TINYINT: {
        int8_t  *in = (int8_t *)pInputData->pData;
        int64_t *out = (int64_t *)pOutputData->pData;
        *out += in[i];
        count++;
        break;
      }
      case TSDB_DATA_TYPE_SMALLINT: {
        int16_t *in = (int16_t *)pInputData->pData;
        int64_t *out = (int64_t *)pOutputData->pData;
        *out += in[i];
        count++;
        break;
      }
      case TSDB_DATA_TYPE_INT: {
        int32_t *in = (int32_t *)pInputData->pData;
        int64_t *out = (int64_t *)pOutputData->pData;
        *out += in[i];
        count++;
        break;
      }
      case TSDB_DATA_TYPE_BIGINT: {
        int64_t *in = (int64_t *)pInputData->pData;
        int64_t *out = (int64_t *)pOutputData->pData;
        *out += in[i];
        count++;
        break;
      }
      case TSDB_DATA_TYPE_UTINYINT: {
        uint8_t  *in = (uint8_t *)pInputData->pData;
        uint64_t *out = (uint64_t *)pOutputData->pData;
        *out += in[i];
        count++;
        break;
      }
      case TSDB_DATA_TYPE_USMALLINT: {
        uint16_t *in = (uint16_t *)pInputData->pData;
        uint64_t *out = (uint64_t *)pOutputData->pData;
        *out += in[i];
        count++;
        break;
      }
      case TSDB_DATA_TYPE_UINT: {
        uint32_t *in = (uint32_t *)pInputData->pData;
        uint64_t *out = (uint64_t *)pOutputData->pData;
        *out += in[i];
        count++;
        break;
      }
      case TSDB_DATA_TYPE_UBIGINT: {
        uint64_t *in = (uint64_t *)pInputData->pData;
        uint64_t *out = (uint64_t *)pOutputData->pData;
        *out += in[i];
        count++;
        break;
      }
      case TSDB_DATA_TYPE_FLOAT: {
        float *in = (float *)pInputData->pData;
        float *out = (float *)pOutputData->pData;
        *out += in[i];
        count++;
        break;
      }
      case TSDB_DATA_TYPE_DOUBLE: {
        double *in = (double *)pInputData->pData;
        double *out = (double *)pOutputData->pData;
        *out += in[i];
        count++;
        break;
      }
    }
  }

  if (hasNull || (count == 0)) {
    colDataSetNULL(pOutputData, 0);
  } else {
    if (IS_SIGNED_NUMERIC_TYPE(type)) {
      int64_t *out = (int64_t *)pOutputData->pData;
      *(double *)out = *out / (double)count;
    } else if (IS_UNSIGNED_NUMERIC_TYPE(type)) {
      uint64_t *out = (uint64_t *)pOutputData->pData;
      *(double *)out = *out / (double)count;
    } else if (IS_FLOAT_TYPE(type)) {
      double *out = (double *)pOutputData->pData;
      *(double *)out = *out / (double)count;
    }
  }

  pOutput->numOfRows = 1;
  return TSDB_CODE_SUCCESS;
}

int32_t stddevScalarFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  SColumnInfoData *pInputData = pInput->columnData;
  SColumnInfoData *pOutputData = pOutput->columnData;

  int32_t type = GET_PARAM_TYPE(pInput);
  // int64_t count = 0, sum = 0, qSum = 0;
  bool hasNull = false;

  for (int32_t i = 0; i < pInput->numOfRows; ++i) {
    if (colDataIsNull_s(pInputData, i)) {
      hasNull = true;
      break;
    }
#if 0
    switch(type) {
      case TSDB_DATA_TYPE_TINYINT: {
        int8_t *in  = (int8_t *)pInputData->pData;
        sum += in[i];
        qSum += in[i] * in[i];
        count++;
        break;
      }
      case TSDB_DATA_TYPE_SMALLINT: {
        int16_t *in  = (int16_t *)pInputData->pData;
        sum += in[i];
        qSum += in[i] * in[i];
        count++;
        break;
      }
      case TSDB_DATA_TYPE_INT: {
        int32_t *in  = (int32_t *)pInputData->pData;
        sum += in[i];
        qSum += in[i] * in[i];
        count++;
        break;
      }
      case TSDB_DATA_TYPE_BIGINT: {
        int64_t *in  = (int64_t *)pInputData->pData;
        sum += in[i];
        qSum += in[i] * in[i];
        count++;
        break;
      }
      case TSDB_DATA_TYPE_UTINYINT: {
        uint8_t *in  = (uint8_t *)pInputData->pData;
        sum += in[i];
        qSum += in[i] * in[i];
        count++;
        break;
      }
      case TSDB_DATA_TYPE_USMALLINT: {
        uint16_t *in  = (uint16_t *)pInputData->pData;
        sum += in[i];
        qSum += in[i] * in[i];
        count++;
        break;
      }
      case TSDB_DATA_TYPE_UINT: {
        uint32_t *in  = (uint32_t *)pInputData->pData;
        sum += in[i];
        qSum += in[i] * in[i];
        count++;
        break;
      }
      case TSDB_DATA_TYPE_UBIGINT: {
        uint64_t *in  = (uint64_t *)pInputData->pData;
        sum += in[i];
        qSum += in[i] * in[i];
        count++;
        break;
      }
      case TSDB_DATA_TYPE_FLOAT: {
        float *in  = (float *)pInputData->pData;
        sum += in[i];
        qSum += in[i] * in[i];
        count++;
        break;
      }
      case TSDB_DATA_TYPE_DOUBLE: {
        double *in  = (double *)pInputData->pData;
        sum += in[i];
        qSum += in[i] * in[i];
        count++;
        break;
      }
    }
#endif
  }

  double *out = (double *)pOutputData->pData;
  if (hasNull) {
    colDataSetNULL(pOutputData, 0);
  } else {
    *out = 0;
#if 0
    double avg = 0;
    if (IS_SIGNED_NUMERIC_TYPE(type)) {
      avg = (int64_t)sum / (double)count;
      *out =  sqrt(fabs((int64_t)qSum / ((double)count) - avg * avg));
    } else if (IS_UNSIGNED_NUMERIC_TYPE(type)) {
      avg = (uint64_t)sum / (double)count;
      *out =  sqrt(fabs((uint64_t)qSum / ((double)count) - avg * avg));
    } else if (IS_FLOAT_TYPE(type)) {
      avg = (double)sum / (double)count;
      *out =  sqrt(fabs((double)qSum / ((double)count) - avg * avg));
    }
#endif
  }

  pOutput->numOfRows = 1;
  return TSDB_CODE_SUCCESS;
}

#define LEASTSQR_CAL(p, x, y, index, step) \
  do {                                     \
    (p)[0][0] += (double)(x) * (x);        \
    (p)[0][1] += (double)(x);              \
    (p)[0][2] += (double)(x) * (y)[index]; \
    (p)[1][2] += (y)[index];               \
    (x) += step;                           \
  } while (0)

int32_t leastSQRScalarFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  SColumnInfoData *pInputData = pInput->columnData;
  SColumnInfoData *pOutputData = pOutput->columnData;

  double startVal, stepVal;
  double matrix[2][3] = {0};
  GET_TYPED_DATA(startVal, double, GET_PARAM_TYPE(&pInput[1]), pInput[1].columnData->pData);
  GET_TYPED_DATA(stepVal, double, GET_PARAM_TYPE(&pInput[2]), pInput[2].columnData->pData);

  int32_t type = GET_PARAM_TYPE(pInput);
  int64_t count = 0;

  switch (type) {
    case TSDB_DATA_TYPE_TINYINT: {
      int8_t *in = (int8_t *)pInputData->pData;
      for (int32_t i = 0; i < pInput->numOfRows; ++i) {
        if (colDataIsNull_s(pInputData, i)) {
          continue;
        }

        count++;
        LEASTSQR_CAL(matrix, startVal, in, i, stepVal);
      }
      break;
    }
    case TSDB_DATA_TYPE_SMALLINT: {
      int16_t *in = (int16_t *)pInputData->pData;
      for (int32_t i = 0; i < pInput->numOfRows; ++i) {
        if (colDataIsNull_s(pInputData, i)) {
          continue;
        }

        count++;
        LEASTSQR_CAL(matrix, startVal, in, i, stepVal);
      }
      break;
    }
    case TSDB_DATA_TYPE_INT: {
      int32_t *in = (int32_t *)pInputData->pData;
      for (int32_t i = 0; i < pInput->numOfRows; ++i) {
        if (colDataIsNull_s(pInputData, i)) {
          continue;
        }

        count++;
        LEASTSQR_CAL(matrix, startVal, in, i, stepVal);
      }
      break;
    }
    case TSDB_DATA_TYPE_BIGINT: {
      int64_t *in = (int64_t *)pInputData->pData;
      for (int32_t i = 0; i < pInput->numOfRows; ++i) {
        if (colDataIsNull_s(pInputData, i)) {
          continue;
        }

        count++;
        LEASTSQR_CAL(matrix, startVal, in, i, stepVal);
      }
      break;
    }
    case TSDB_DATA_TYPE_UTINYINT: {
      uint8_t *in = (uint8_t *)pInputData->pData;
      for (int32_t i = 0; i < pInput->numOfRows; ++i) {
        if (colDataIsNull_s(pInputData, i)) {
          continue;
        }

        count++;
        LEASTSQR_CAL(matrix, startVal, in, i, stepVal);
      }
      break;
    }
    case TSDB_DATA_TYPE_USMALLINT: {
      uint16_t *in = (uint16_t *)pInputData->pData;
      for (int32_t i = 0; i < pInput->numOfRows; ++i) {
        if (colDataIsNull_s(pInputData, i)) {
          continue;
        }

        count++;
        LEASTSQR_CAL(matrix, startVal, in, i, stepVal);
      }
      break;
    }
    case TSDB_DATA_TYPE_UINT: {
      uint32_t *in = (uint32_t *)pInputData->pData;
      for (int32_t i = 0; i < pInput->numOfRows; ++i) {
        if (colDataIsNull_s(pInputData, i)) {
          continue;
        }

        count++;
        LEASTSQR_CAL(matrix, startVal, in, i, stepVal);
      }
      break;
    }
    case TSDB_DATA_TYPE_UBIGINT: {
      uint64_t *in = (uint64_t *)pInputData->pData;
      for (int32_t i = 0; i < pInput->numOfRows; ++i) {
        if (colDataIsNull_s(pInputData, i)) {
          continue;
        }

        count++;
        LEASTSQR_CAL(matrix, startVal, in, i, stepVal);
      }
      break;
    }
    case TSDB_DATA_TYPE_FLOAT: {
      float *in = (float *)pInputData->pData;
      for (int32_t i = 0; i < pInput->numOfRows; ++i) {
        if (colDataIsNull_s(pInputData, i)) {
          continue;
        }

        count++;
        LEASTSQR_CAL(matrix, startVal, in, i, stepVal);
      }
      break;
    }
    case TSDB_DATA_TYPE_DOUBLE: {
      double *in = (double *)pInputData->pData;
      for (int32_t i = 0; i < pInput->numOfRows; ++i) {
        if (colDataIsNull_s(pInputData, i)) {
          continue;
        }

        count++;
        LEASTSQR_CAL(matrix, startVal, in, i, stepVal);
      }
      break;
    }
  }

  if (count == 0) {
    colDataSetNULL(pOutputData, 0);
  } else {
    matrix[1][1] = (double)count;
    matrix[1][0] = matrix[0][1];

    double matrix00 = matrix[0][0] - matrix[1][0] * (matrix[0][1] / matrix[1][1]);
    double matrix02 = matrix[0][2] - matrix[1][2] * (matrix[0][1] / matrix[1][1]);
    double matrix12 = matrix[1][2] - matrix02 * (matrix[1][0] / matrix00);
    matrix02 /= matrix00;

    matrix12 /= matrix[1][1];

    char buf[LEASTSQUARES_BUFF_LENGTH] = {0};
    char slopBuf[64] = {0};
    char interceptBuf[64] = {0};
    int  n = snprintf(slopBuf, 64, "%.6lf", matrix02);
    if (n > LEASTSQUARES_DOUBLE_ITEM_LENGTH) {
      snprintf(slopBuf, 64, "%." DOUBLE_PRECISION_DIGITS, matrix02);
    }
    n = snprintf(interceptBuf, 64, "%.6lf", matrix12);
    if (n > LEASTSQUARES_DOUBLE_ITEM_LENGTH) {
      snprintf(interceptBuf, 64, "%." DOUBLE_PRECISION_DIGITS, matrix12);
    }
    size_t len =
        snprintf(varDataVal(buf), sizeof(buf) - VARSTR_HEADER_SIZE, "{slop:%s, intercept:%s}", slopBuf, interceptBuf);
    varDataSetLen(buf, len);
    colDataSetVal(pOutputData, 0, buf, false);
  }

  pOutput->numOfRows = 1;
  return TSDB_CODE_SUCCESS;
}

int32_t percentileScalarFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  SColumnInfoData *pInputData = pInput->columnData;
  SColumnInfoData *pOutputData = pOutput->columnData;

  int32_t type = GET_PARAM_TYPE(pInput);

  double val;
  bool   hasNull = false;
  for (int32_t i = 0; i < pInput->numOfRows; ++i) {
    if (colDataIsNull_s(pInputData, i)) {
      hasNull = true;
      break;
    }
    char *in = pInputData->pData;
    GET_TYPED_DATA(val, double, type, in);
  }

  if (hasNull) {
    colDataSetNULL(pOutputData, 0);
  } else {
    colDataSetVal(pOutputData, 0, (char *)&val, false);
  }

  pOutput->numOfRows = 1;
  return TSDB_CODE_SUCCESS;
}

int32_t apercentileScalarFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  return percentileScalarFunction(pInput, inputNum, pOutput);
}

int32_t spreadScalarFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  SColumnInfoData *pInputData = pInput->columnData;
  SColumnInfoData *pOutputData = pOutput->columnData;

  int32_t type = GET_PARAM_TYPE(pInput);

  double min, max;
  SET_DOUBLE_VAL(&min, DBL_MAX);
  SET_DOUBLE_VAL(&max, -DBL_MAX);

  bool hasNull = false;
  for (int32_t i = 0; i < pInput->numOfRows; ++i) {
    if (colDataIsNull_s(pInputData, i)) {
      hasNull = true;
      break;
    }

    char *in = pInputData->pData;

    double val = 0;
    GET_TYPED_DATA(val, double, type, in);

    if (val < GET_DOUBLE_VAL(&min)) {
      SET_DOUBLE_VAL(&min, val);
    }

    if (val > GET_DOUBLE_VAL(&max)) {
      SET_DOUBLE_VAL(&max, val);
    }
  }

  if (hasNull) {
    colDataSetNULL(pOutputData, 0);
  } else {
    double result = max - min;
    colDataSetVal(pOutputData, 0, (char *)&result, false);
  }

  pOutput->numOfRows = 1;
  return TSDB_CODE_SUCCESS;
}

int32_t nonCalcScalarFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  SColumnInfoData *pInputData = pInput->columnData;
  SColumnInfoData *pOutputData = pOutput->columnData;

  int32_t type = GET_PARAM_TYPE(pInput);
  bool    hasNull = false;

  for (int32_t i = 0; i < pInput->numOfRows; ++i) {
    if (colDataIsNull_s(pInputData, i)) {
      hasNull = true;
      break;
    }
  }

  double *out = (double *)pOutputData->pData;
  if (hasNull) {
    colDataSetNULL(pOutputData, 0);
  } else {
    *out = 0;
  }

  pOutput->numOfRows = 1;
  return TSDB_CODE_SUCCESS;
}

int32_t derivativeScalarFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  return nonCalcScalarFunction(pInput, inputNum, pOutput);
}

int32_t irateScalarFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  return nonCalcScalarFunction(pInput, inputNum, pOutput);
}

int32_t diffScalarFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  return nonCalcScalarFunction(pInput, inputNum, pOutput);
}

int32_t twaScalarFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  return avgScalarFunction(pInput, inputNum, pOutput);
}

int32_t mavgScalarFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  return avgScalarFunction(pInput, inputNum, pOutput);
}

int32_t hllScalarFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  return countScalarFunction(pInput, inputNum, pOutput);
}

int32_t csumScalarFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  return sumScalarFunction(pInput, inputNum, pOutput);
}

typedef enum {
  STATE_OPER_INVALID = 0,
  STATE_OPER_LT,
  STATE_OPER_GT,
  STATE_OPER_LE,
  STATE_OPER_GE,
  STATE_OPER_NE,
  STATE_OPER_EQ,
} EStateOperType;

#define STATE_COMP(_op, _lval, _rval, _rtype) STATE_COMP_IMPL(_op, _lval, GET_STATE_VAL(_rval, _rtype))

#define GET_STATE_VAL(_val, _type) ((_type == TSDB_DATA_TYPE_BIGINT) ? (*(int64_t *)_val) : (*(double *)_val))

#define STATE_COMP_IMPL(_op, _lval, _rval) \
  do {                                     \
    switch (_op) {                         \
      case STATE_OPER_LT:                  \
        return ((_lval) < (_rval));        \
        break;                             \
      case STATE_OPER_GT:                  \
        return ((_lval) > (_rval));        \
        break;                             \
      case STATE_OPER_LE:                  \
        return ((_lval) <= (_rval));       \
        break;                             \
      case STATE_OPER_GE:                  \
        return ((_lval) >= (_rval));       \
        break;                             \
      case STATE_OPER_NE:                  \
        return ((_lval) != (_rval));       \
        break;                             \
      case STATE_OPER_EQ:                  \
        return ((_lval) == (_rval));       \
        break;                             \
      default:                             \
        break;                             \
    }                                      \
  } while (0)

static int8_t getStateOpType(char *opStr) {
  int8_t opType;
  if (strncasecmp(opStr, "LT", 2) == 0) {
    opType = STATE_OPER_LT;
  } else if (strncasecmp(opStr, "GT", 2) == 0) {
    opType = STATE_OPER_GT;
  } else if (strncasecmp(opStr, "LE", 2) == 0) {
    opType = STATE_OPER_LE;
  } else if (strncasecmp(opStr, "GE", 2) == 0) {
    opType = STATE_OPER_GE;
  } else if (strncasecmp(opStr, "NE", 2) == 0) {
    opType = STATE_OPER_NE;
  } else if (strncasecmp(opStr, "EQ", 2) == 0) {
    opType = STATE_OPER_EQ;
  } else {
    opType = STATE_OPER_INVALID;
  }

  return opType;
}

static bool checkStateOp(int8_t op, SColumnInfoData *pCol, int32_t index, SScalarParam *pCondParam) {
  char   *data = colDataGetData(pCol, index);
  char   *param = pCondParam->columnData->pData;
  int32_t paramType = GET_PARAM_TYPE(pCondParam);
  switch (pCol->info.type) {
    case TSDB_DATA_TYPE_TINYINT: {
      int8_t v = *(int8_t *)data;
      STATE_COMP(op, v, param, paramType);
      break;
    }
    case TSDB_DATA_TYPE_UTINYINT: {
      uint8_t v = *(uint8_t *)data;
      STATE_COMP(op, v, param, paramType);
      break;
    }
    case TSDB_DATA_TYPE_SMALLINT: {
      int16_t v = *(int16_t *)data;
      STATE_COMP(op, v, param, paramType);
      break;
    }
    case TSDB_DATA_TYPE_USMALLINT: {
      uint16_t v = *(uint16_t *)data;
      STATE_COMP(op, v, param, paramType);
      break;
    }
    case TSDB_DATA_TYPE_INT: {
      int32_t v = *(int32_t *)data;
      STATE_COMP(op, v, param, paramType);
      break;
    }
    case TSDB_DATA_TYPE_UINT: {
      uint32_t v = *(uint32_t *)data;
      STATE_COMP(op, v, param, paramType);
      break;
    }
    case TSDB_DATA_TYPE_BIGINT: {
      int64_t v = *(int64_t *)data;
      STATE_COMP(op, v, param, paramType);
      break;
    }
    case TSDB_DATA_TYPE_UBIGINT: {
      uint64_t v = *(uint64_t *)data;
      STATE_COMP(op, v, param, paramType);
      break;
    }
    case TSDB_DATA_TYPE_FLOAT: {
      float v = *(float *)data;
      STATE_COMP(op, v, param, paramType);
      break;
    }
    case TSDB_DATA_TYPE_DOUBLE: {
      double v = *(double *)data;
      STATE_COMP(op, v, param, paramType);
      break;
    }
    default: {
      return false;
    }
  }
  return false;
}

int32_t stateCountScalarFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  SColumnInfoData *pInputData = pInput->columnData;
  SColumnInfoData *pOutputData = pOutput->columnData;

  int8_t  op = getStateOpType(varDataVal(pInput[1].columnData->pData));
  int64_t count = 0;

  for (int32_t i = 0; i < pInput->numOfRows; ++i) {
    if (colDataIsNull_s(pInputData, i)) {
      colDataSetNULL(pOutputData, i);
      continue;
    }

    bool    ret = checkStateOp(op, pInputData, i, &pInput[2]);
    int64_t out = -1;
    if (ret) {
      out = ++count;
    } else {
      count = 0;
    }
    colDataSetVal(pOutputData, i, (char *)&out, false);
  }

  pOutput->numOfRows = pInput->numOfRows;
  return TSDB_CODE_SUCCESS;
}

int32_t stateDurationScalarFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  SColumnInfoData *pInputData = pInput->columnData;
  SColumnInfoData *pOutputData = pOutput->columnData;

  int8_t op = getStateOpType(varDataVal(pInput[1].columnData->pData));

  for (int32_t i = 0; i < pInput->numOfRows; ++i) {
    if (colDataIsNull_s(pInputData, i)) {
      colDataSetNULL(pOutputData, i);
      continue;
    }

    bool    ret = checkStateOp(op, pInputData, i, &pInput[2]);
    int64_t out = -1;
    if (ret) {
      out = 0;
    }
    colDataSetVal(pOutputData, i, (char *)&out, false);
  }

  pOutput->numOfRows = pInput->numOfRows;
  return TSDB_CODE_SUCCESS;
}

typedef enum { UNKNOWN_BIN = 0, USER_INPUT_BIN, LINEAR_BIN, LOG_BIN } EHistoBinType;

static int8_t getHistogramBinType(char *binTypeStr) {
  int8_t binType;
  if (strcasecmp(binTypeStr, "user_input") == 0) {
    binType = USER_INPUT_BIN;
  } else if (strcasecmp(binTypeStr, "linear_bin") == 0) {
    binType = LINEAR_BIN;
  } else if (strcasecmp(binTypeStr, "log_bin") == 0) {
    binType = LOG_BIN;
  } else {
    binType = UNKNOWN_BIN;
  }

  return binType;
}

typedef struct SHistoFuncBin {
  double  lower;
  double  upper;
  int64_t count;
  double  percentage;
} SHistoFuncBin;

static bool getHistogramBinDesc(SHistoFuncBin **bins, int32_t *binNum, char *binDescStr, int8_t binType,
                                bool normalized) {
  cJSON  *binDesc = cJSON_Parse(binDescStr);
  int32_t numOfBins;
  double *intervals;
  if (cJSON_IsObject(binDesc)) { /* linaer/log bins */
    int32_t numOfParams = cJSON_GetArraySize(binDesc);
    int32_t startIndex;
    if (numOfParams != 4) {
      cJSON_Delete(binDesc);
      return false;
    }

    cJSON *start = cJSON_GetObjectItem(binDesc, "start");
    cJSON *factor = cJSON_GetObjectItem(binDesc, "factor");
    cJSON *width = cJSON_GetObjectItem(binDesc, "width");
    cJSON *count = cJSON_GetObjectItem(binDesc, "count");
    cJSON *infinity = cJSON_GetObjectItem(binDesc, "infinity");

    if (!cJSON_IsNumber(start) || !cJSON_IsNumber(count) || !cJSON_IsBool(infinity)) {
      cJSON_Delete(binDesc);
      return false;
    }

    if (count->valueint <= 0 || count->valueint > 1000) {  // limit count to 1000
      cJSON_Delete(binDesc);
      return false;
    }

    if (isinf(start->valuedouble) || (width != NULL && isinf(width->valuedouble)) ||
        (factor != NULL && isinf(factor->valuedouble)) || (count != NULL && isinf(count->valuedouble))) {
      cJSON_Delete(binDesc);
      return false;
    }

    int32_t counter = (int32_t)count->valueint;
    if (infinity->valueint == false) {
      startIndex = 0;
      numOfBins = counter + 1;
    } else {
      startIndex = 1;
      numOfBins = counter + 3;
    }

    intervals = taosMemoryCalloc(numOfBins, sizeof(double));
    if (cJSON_IsNumber(width) && factor == NULL && binType == LINEAR_BIN) {
      // linear bin process
      if (width->valuedouble == 0) {
        taosMemoryFree(intervals);
        cJSON_Delete(binDesc);
        return false;
      }
      for (int i = 0; i < counter + 1; ++i) {
        intervals[startIndex] = start->valuedouble + i * width->valuedouble;
        if (isinf(intervals[startIndex])) {
          taosMemoryFree(intervals);
          cJSON_Delete(binDesc);
          return false;
        }
        startIndex++;
      }
    } else if (cJSON_IsNumber(factor) && width == NULL && binType == LOG_BIN) {
      // log bin process
      if (start->valuedouble == 0) {
        taosMemoryFree(intervals);
        cJSON_Delete(binDesc);
        return false;
      }
      if (factor->valuedouble < 0 || factor->valuedouble == 0 || factor->valuedouble == 1) {
        taosMemoryFree(intervals);
        cJSON_Delete(binDesc);
        return false;
      }
      for (int i = 0; i < counter + 1; ++i) {
        intervals[startIndex] = start->valuedouble * pow(factor->valuedouble, i * 1.0);
        if (isinf(intervals[startIndex])) {
          taosMemoryFree(intervals);
          cJSON_Delete(binDesc);
          return false;
        }
        startIndex++;
      }
    } else {
      taosMemoryFree(intervals);
      cJSON_Delete(binDesc);
      return false;
    }

    if (infinity->valueint == true) {
      intervals[0] = -INFINITY;
      intervals[numOfBins - 1] = INFINITY;
      // in case of desc bin orders, -inf/inf should be swapped
      if (numOfBins < 4) {
        return false;
      }
      if (intervals[1] > intervals[numOfBins - 2]) {
        TSWAP(intervals[0], intervals[numOfBins - 1]);
      }
    }
  } else if (cJSON_IsArray(binDesc)) { /* user input bins */
    if (binType != USER_INPUT_BIN) {
      cJSON_Delete(binDesc);
      return false;
    }
    numOfBins = cJSON_GetArraySize(binDesc);
    intervals = taosMemoryCalloc(numOfBins, sizeof(double));
    cJSON *bin = binDesc->child;
    if (bin == NULL) {
      taosMemoryFree(intervals);
      cJSON_Delete(binDesc);
      return false;
    }
    int i = 0;
    while (bin) {
      intervals[i] = bin->valuedouble;
      if (!cJSON_IsNumber(bin)) {
        taosMemoryFree(intervals);
        cJSON_Delete(binDesc);
        return false;
      }
      if (i != 0 && intervals[i] <= intervals[i - 1]) {
        taosMemoryFree(intervals);
        cJSON_Delete(binDesc);
        return false;
      }
      bin = bin->next;
      i++;
    }
  } else {
    cJSON_Delete(binDesc);
    return false;
  }

  *binNum = numOfBins - 1;
  *bins = taosMemoryCalloc(numOfBins, sizeof(SHistoFuncBin));
  for (int32_t i = 0; i < *binNum; ++i) {
    (*bins)[i].lower = intervals[i] < intervals[i + 1] ? intervals[i] : intervals[i + 1];
    (*bins)[i].upper = intervals[i + 1] > intervals[i] ? intervals[i + 1] : intervals[i];
    (*bins)[i].count = 0;
  }

  taosMemoryFree(intervals);
  cJSON_Delete(binDesc);

  return true;
}

int32_t histogramScalarFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  SColumnInfoData *pInputData = pInput->columnData;
  SColumnInfoData *pOutputData = pOutput->columnData;

  SHistoFuncBin *bins;
  int32_t        numOfBins = 0;
  int32_t        totalCount = 0;

  char *binTypeStr = strndup(varDataVal(pInput[1].columnData->pData), varDataLen(pInput[1].columnData->pData));
  int8_t binType = getHistogramBinType(binTypeStr);
  taosMemoryFree(binTypeStr);

  char   *binDesc = strndup(varDataVal(pInput[2].columnData->pData), varDataLen(pInput[2].columnData->pData));
  int64_t normalized = *(int64_t *)(pInput[3].columnData->pData);

  int32_t type = GET_PARAM_TYPE(pInput);
  if (!getHistogramBinDesc(&bins, &numOfBins, binDesc, binType, (bool)normalized)) {
    taosMemoryFree(binDesc);
    return TSDB_CODE_FAILED;
  }
  taosMemoryFree(binDesc);

  for (int32_t i = 0; i < pInput->numOfRows; ++i) {
    if (colDataIsNull_s(pInputData, i)) {
      continue;
    }

    char  *data = colDataGetData(pInputData, i);
    double v;
    GET_TYPED_DATA(v, double, type, data);

    for (int32_t k = 0; k < numOfBins; ++k) {
      if (v > bins[k].lower && v <= bins[k].upper) {
        bins[k].count++;
        totalCount++;
        break;
      }
    }
  }

  if (normalized) {
    for (int32_t k = 0; k < numOfBins; ++k) {
      if (totalCount != 0) {
        bins[k].percentage = bins[k].count / (double)totalCount;
      } else {
        bins[k].percentage = 0;
      }
    }
  }

  colInfoDataEnsureCapacity(pOutputData, numOfBins, false);

  for (int32_t k = 0; k < numOfBins; ++k) {
    int32_t len;
    char    buf[512] = {0};
    if (!normalized) {
      len = sprintf(varDataVal(buf), "{\"lower_bin\":%g, \"upper_bin\":%g, \"count\":%" PRId64 "}", bins[k].lower,
                    bins[k].upper, bins[k].count);
    } else {
      len = sprintf(varDataVal(buf), "{\"lower_bin\":%g, \"upper_bin\":%g, \"count\":%lf}", bins[k].lower,
                    bins[k].upper, bins[k].percentage);
    }
    varDataSetLen(buf, len);
    colDataSetVal(pOutputData, k, buf, false);
  }

  taosMemoryFree(bins);
  pOutput->numOfRows = numOfBins;
  return TSDB_CODE_SUCCESS;
}

int32_t selectScalarFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  SColumnInfoData *pInputData = pInput->columnData;
  SColumnInfoData *pOutputData = pOutput->columnData;

  int32_t type = GET_PARAM_TYPE(pInput);

  for (int32_t i = 0; i < pInput->numOfRows; ++i) {
    if (colDataIsNull_s(pInputData, i)) {
      colDataSetNULL(pOutputData, 0);
      continue;
    }

    char *data = colDataGetData(pInputData, i);
    colDataSetVal(pOutputData, i, data, false);
  }

  pOutput->numOfRows = 1;
  return TSDB_CODE_SUCCESS;
}

int32_t topBotScalarFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  return selectScalarFunction(pInput, inputNum, pOutput);
}

int32_t firstLastScalarFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  return selectScalarFunction(pInput, inputNum, pOutput);
}

int32_t sampleScalarFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  return selectScalarFunction(pInput, inputNum, pOutput);
}

int32_t tailScalarFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  return selectScalarFunction(pInput, inputNum, pOutput);
}

int32_t uniqueScalarFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  return selectScalarFunction(pInput, inputNum, pOutput);
}

int32_t modeScalarFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  return selectScalarFunction(pInput, inputNum, pOutput);
}
