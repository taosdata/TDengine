#include "function.h"
#include "scalar.h"
#include "tdatablock.h"
#include "sclInt.h"
#include "sclvector.h"

typedef float (*_float_fn)(float);
typedef double (*_double_fn)(double);
typedef double (*_double_fn_2)(double, double);
typedef int (*_conv_fn)(int);
typedef void (*_trim_fn)(char *, char*, int32_t, int32_t);
typedef int16_t (*_len_fn)(char *, int32_t);

/** Math functions **/
static double tlog(double v, double base) {
  return log(v) / log(base);
}

int32_t absFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  SColumnInfoData *pInputData  = pInput->columnData;
  SColumnInfoData *pOutputData = pOutput->columnData;

  int32_t type = GET_PARAM_TYPE(pInput);
  if (!IS_NUMERIC_TYPE(type)) {
    return TSDB_CODE_FAILED;
  }

  switch (type) {
    case TSDB_DATA_TYPE_FLOAT: {
      float *in  = (float *)pInputData->pData;
      float *out = (float *)pOutputData->pData;
      for (int32_t i = 0; i < pInput->numOfRows; ++i) {
        if (colDataIsNull_f(pInputData->nullbitmap, i)) {
          colDataSetNull_f(pOutputData->nullbitmap, i);
          continue;
        }
        out[i] = (in[i] >= 0)? in[i] : -in[i];
      }
      break;
    }

    case TSDB_DATA_TYPE_DOUBLE: {
      double *in  = (double *)pInputData->pData;
      double *out = (double *)pOutputData->pData;
      for (int32_t i = 0; i < pInput->numOfRows; ++i) {
        if (colDataIsNull_f(pInputData->nullbitmap, i)) {
          colDataSetNull_f(pOutputData->nullbitmap, i);
          continue;
        }
        out[i] = (in[i] >= 0)? in[i] : -in[i];
      }
      break;
    }

    case TSDB_DATA_TYPE_TINYINT: {
      int8_t *in  = (int8_t *)pInputData->pData;
      int8_t *out = (int8_t *)pOutputData->pData;
      for (int32_t i = 0; i < pInput->numOfRows; ++i) {
        if (colDataIsNull_f(pInputData->nullbitmap, i)) {
          colDataSetNull_f(pOutputData->nullbitmap, i);
          continue;
        }
        out[i] = (in[i] >= 0)? in[i] : -in[i];
      }
      break;
    }

    case TSDB_DATA_TYPE_SMALLINT: {
      int16_t *in  = (int16_t *)pInputData->pData;
      int16_t *out = (int16_t *)pOutputData->pData;
      for (int32_t i = 0; i < pInput->numOfRows; ++i) {
        if (colDataIsNull_f(pInputData->nullbitmap, i)) {
          colDataSetNull_f(pOutputData->nullbitmap, i);
          continue;
        }
        out[i] = (in[i] >= 0)? in[i] : -in[i];
      }
      break;
    }

    case TSDB_DATA_TYPE_INT: {
      int32_t *in  = (int32_t *)pInputData->pData;
      int32_t *out = (int32_t *)pOutputData->pData;
      for (int32_t i = 0; i < pInput->numOfRows; ++i) {
        if (colDataIsNull_f(pInputData->nullbitmap, i)) {
          colDataSetNull_f(pOutputData->nullbitmap, i);
          continue;
        }
        out[i] = (in[i] >= 0)? in[i] : -in[i];
      }
      break;
    }

    case TSDB_DATA_TYPE_BIGINT: {
      int64_t *in  = (int64_t *)pInputData->pData;
      int64_t *out = (int64_t *)pOutputData->pData;
      for (int32_t i = 0; i < pInput->numOfRows; ++i) {
        if (colDataIsNull_f(pInputData->nullbitmap, i)) {
          colDataSetNull_f(pOutputData->nullbitmap, i);
          continue;
        }
        out[i] = (in[i] >= 0)? in[i] : -in[i];
      }
      break;
    }

    default: {
      colDataAssign(pOutputData, pInputData, pInput->numOfRows);
    }
  }

  pOutput->numOfRows = pInput->numOfRows;
  return TSDB_CODE_SUCCESS;
}

static int32_t doScalarFunctionUnique(SScalarParam *pInput, int32_t inputNum, SScalarParam* pOutput, _double_fn valFn) {
  int32_t type = GET_PARAM_TYPE(pInput);
  if (inputNum != 1 || !IS_NUMERIC_TYPE(type)) {
    return TSDB_CODE_FAILED;
  }

  SColumnInfoData *pInputData = pInput->columnData;
  SColumnInfoData *pOutputData = pOutput->columnData;

  _getDoubleValue_fn_t getValueFn = getVectorDoubleValueFn(type);

  double *out = (double *)pOutputData->pData;

  for (int32_t i = 0; i < pInput->numOfRows; ++i) {
    if (colDataIsNull_f(pInputData->nullbitmap, i)) {
      colDataSetNull_f(pOutputData->nullbitmap, i);
      continue;
    }
    out[i] = valFn(getValueFn(pInputData->pData, i));
  }

  pOutput->numOfRows = pInput->numOfRows;
  return TSDB_CODE_SUCCESS;
}

static int32_t doScalarFunctionUnique2(SScalarParam *pInput, int32_t inputNum, SScalarParam* pOutput, _double_fn_2 valFn) {
  if (inputNum != 2 || !IS_NUMERIC_TYPE(GET_PARAM_TYPE(&pInput[0])) || !IS_NUMERIC_TYPE(GET_PARAM_TYPE(&pInput[1]))) {
    return TSDB_CODE_FAILED;
  }

  SColumnInfoData *pInputData[2];
  SColumnInfoData *pOutputData = pOutput->columnData;
  _getDoubleValue_fn_t getValueFn[2];

  for (int32_t i = 0; i < inputNum; ++i) {
    pInputData[i] = pInput[i].columnData;
    getValueFn[i]= getVectorDoubleValueFn(GET_PARAM_TYPE(&pInput[i]));
  }

  double *out = (double *)pOutputData->pData;

  for (int32_t i = 0; i < pInput->numOfRows; ++i) {
    if (colDataIsNull_f(pInputData[0]->nullbitmap, i) ||
        colDataIsNull_f(pInputData[1]->nullbitmap, 0)) {
      colDataSetNull_f(pOutputData->nullbitmap, i);
      continue;
    }
    out[i] = valFn(getValueFn[0](pInputData[0]->pData, i), getValueFn[1](pInputData[1]->pData, 0));
  }

  pOutput->numOfRows = pInput->numOfRows;
  return TSDB_CODE_SUCCESS;
}

static int32_t doScalarFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam* pOutput, _float_fn f1, _double_fn d1) {
  int32_t type = GET_PARAM_TYPE(pInput);
  if (inputNum != 1 || !IS_NUMERIC_TYPE(type)) {
    return TSDB_CODE_FAILED;
  }

  SColumnInfoData *pInputData  = pInput->columnData;
  SColumnInfoData *pOutputData = pOutput->columnData;

  switch (type) {
    case TSDB_DATA_TYPE_FLOAT: {
      float *in  = (float *)pInputData->pData;
      float *out = (float *)pOutputData->pData;

      for (int32_t i = 0; i < pInput->numOfRows; ++i) {
        if (colDataIsNull_f(pInputData->nullbitmap, i)) {
          colDataSetNull_f(pOutputData->nullbitmap, i);
          continue;
        }
        out[i] = f1(in[i]);
      }
      break;
    }

    case TSDB_DATA_TYPE_DOUBLE: {
      double *in  = (double *)pInputData->pData;
      double *out = (double *)pOutputData->pData;

      for (int32_t i = 0; i < pInput->numOfRows; ++i) {
        if (colDataIsNull_f(pInputData->nullbitmap, i)) {
          colDataSetNull_f(pOutputData->nullbitmap, i);
          continue;
        }
        out[i] = d1(in[i]);
      }
      break;
    }

    default: {
      colDataAssign(pOutputData, pInputData, pInput->numOfRows);
    }
  }

  pOutput->numOfRows = pInput->numOfRows;
  return TSDB_CODE_SUCCESS;
}

/** String functions **/
static int16_t tlength(char *input, int32_t type) {
  return varDataLen(input);
}

static int16_t tcharlength(char *input, int32_t type) {
  if (type == TSDB_DATA_TYPE_VARCHAR) {
    return varDataLen(input);
  } else { //NCHAR
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
  } else { //NCHAR
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
  } else { //NCHAR
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
  if (inputNum != 1 || !IS_VAR_DATA_TYPE(type)) {
    return TSDB_CODE_FAILED;
  }

  SColumnInfoData *pInputData  = pInput->columnData;
  SColumnInfoData *pOutputData = pOutput->columnData;

  char *in = pInputData->pData + pInputData->varmeta.offset[0];
  int16_t *out = (int16_t *)pOutputData->pData;

  for (int32_t i = 0; i < pInput->numOfRows; ++i) {
    if (colDataIsNull_s(pInputData, i)) {
      colDataSetNull_f(pOutputData->nullbitmap, i);
      continue;
    }

    out[i] = lenFn(in, type);
    in += varDataTLen(in);
  }

  pOutput->numOfRows = pInput->numOfRows;
  return TSDB_CODE_SUCCESS;
}

static int32_t concatCopyHelper(const char *input, char *output, bool hasNcharCol, int32_t type, int16_t *dataLen) {
  if (hasNcharCol && type == TSDB_DATA_TYPE_VARCHAR) {
    TdUcs4 *newBuf = taosMemoryCalloc((varDataLen(input) + 1) * TSDB_NCHAR_SIZE, 1);
    bool ret = taosMbsToUcs4(varDataVal(input), varDataLen(input), newBuf, (varDataLen(input) + 1) * TSDB_NCHAR_SIZE, NULL);
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

int32_t concatFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  if (inputNum < 2 || inputNum > 8) { // concat accpet 2-8 input strings
    return TSDB_CODE_FAILED;
  }

  SColumnInfoData **pInputData = taosMemoryCalloc(inputNum, sizeof(SColumnInfoData *));
  SColumnInfoData *pOutputData = pOutput->columnData;
  char **input = taosMemoryCalloc(inputNum, POINTER_BYTES);
  char *outputBuf = NULL;

  int32_t inputLen = 0;
  int32_t numOfRows = 0;
  bool hasNcharCol = false;
  for (int32_t i = 0; i < inputNum; ++i) {
    int32_t type = GET_PARAM_TYPE(&pInput[i]);
    if (!IS_VAR_DATA_TYPE(type)) {
      return TSDB_CODE_FAILED;
    }
    if (type == TSDB_DATA_TYPE_NCHAR) {
      hasNcharCol = true;
    }
    if (pInput[i].numOfRows > numOfRows) {
      numOfRows = pInput[i].numOfRows;
    }
  }
  for (int32_t i = 0; i < inputNum; ++i) {
    pInputData[i] = pInput[i].columnData;
    input[i] = pInputData[i]->pData + pInputData[i]->varmeta.offset[0];
    int32_t factor = 1;
    if (hasNcharCol && (GET_PARAM_TYPE(&pInput[i]) == TSDB_DATA_TYPE_VARCHAR)) {
      factor = TSDB_NCHAR_SIZE;
    }
    if (pInput[i].numOfRows == 1) {
      inputLen += (pInputData[i]->varmeta.length - VARSTR_HEADER_SIZE) * factor * numOfRows;
    } else {
      inputLen += pInputData[i]->varmeta.length - numOfRows * VARSTR_HEADER_SIZE;
    }
  }

  int32_t outputLen = inputLen + numOfRows * VARSTR_HEADER_SIZE;
  outputBuf = taosMemoryCalloc(outputLen, 1);
  char *output = outputBuf;

  bool hasNull = false;
  for (int32_t k = 0; k < numOfRows; ++k) {
    for (int32_t i = 0; i < inputNum; ++i) {
      if (colDataIsNull_s(pInputData[i], k)) {
        colDataAppendNULL(pOutputData, k);
        hasNull = true;
        break;
      }
    }

    if (hasNull) {
      continue;
    }

    int16_t dataLen = 0;
    for (int32_t i = 0; i < inputNum; ++i) {
      int32_t ret = concatCopyHelper(input[i], output, hasNcharCol, GET_PARAM_TYPE(&pInput[i]), &dataLen);
      if (ret != TSDB_CODE_SUCCESS) {
        return ret;
      }
      if (pInput[i].numOfRows != 1) {
        input[i] += varDataTLen(input[i]);
      }
    }
    varDataSetLen(output, dataLen);
    colDataAppend(pOutputData, k, output, false);
    output += varDataTLen(output);
  }

  pOutput->numOfRows = numOfRows;
  taosMemoryFree(input);
  taosMemoryFree(outputBuf);
  taosMemoryFree(pInputData);

  return TSDB_CODE_SUCCESS;
}


int32_t concatWsFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  if (inputNum < 3 || inputNum > 9) { // concat accpet 3-9 input strings including the separator
    return TSDB_CODE_FAILED;
  }

  SColumnInfoData **pInputData = taosMemoryCalloc(inputNum, sizeof(SColumnInfoData *));
  SColumnInfoData *pOutputData = pOutput->columnData;
  char **input = taosMemoryCalloc(inputNum, POINTER_BYTES);
  char *outputBuf = NULL;

  int32_t inputLen = 0;
  int32_t numOfRows = 0;
  bool hasNcharCol = false;
  for (int32_t i = 1; i < inputNum; ++i) {
    int32_t type = GET_PARAM_TYPE(&pInput[i]);
    if (!IS_VAR_DATA_TYPE(GET_PARAM_TYPE(&pInput[i]))) {
      return TSDB_CODE_FAILED;
    }
    if (type == TSDB_DATA_TYPE_NCHAR) {
      hasNcharCol = true;
    }
    if (pInput[i].numOfRows > numOfRows) {
      numOfRows = pInput[i].numOfRows;
    }
  }
  for (int32_t i = 0; i < inputNum; ++i) {
    pInputData[i] = pInput[i].columnData;
    input[i] = pInputData[i]->pData + pInputData[i]->varmeta.offset[0];
    int32_t factor = 1;
    if (hasNcharCol && (GET_PARAM_TYPE(&pInput[i]) == TSDB_DATA_TYPE_VARCHAR)) {
      factor = TSDB_NCHAR_SIZE;
    }
    if (i == 0) {
      // calculate required separator space
      inputLen += (pInputData[0]->varmeta.length - VARSTR_HEADER_SIZE) * numOfRows * (inputNum - 2) * factor;
    } else if (pInput[i].numOfRows == 1) {
      inputLen += (pInputData[i]->varmeta.length - VARSTR_HEADER_SIZE) * numOfRows * factor;
    } else {
      inputLen += pInputData[i]->varmeta.length - numOfRows * VARSTR_HEADER_SIZE;
    }
  }

  int32_t outputLen = inputLen + numOfRows * VARSTR_HEADER_SIZE;
  outputBuf = taosMemoryCalloc(outputLen, 1);
  char *output = outputBuf;

  for (int32_t k = 0; k < numOfRows; ++k) {
    if (colDataIsNull_s(pInputData[0], k)) {
      colDataAppendNULL(pOutputData, k);
      continue;
    }

    int16_t dataLen = 0;
    for (int32_t i = 1; i < inputNum; ++i) {
      if (colDataIsNull_s(pInputData[i], k)) {
        continue;
      }

      int32_t ret = concatCopyHelper(input[i], output, hasNcharCol, GET_PARAM_TYPE(&pInput[i]), &dataLen);
      if (ret != TSDB_CODE_SUCCESS) {
        return ret;
      }

      if (pInput[i].numOfRows != 1) {
        input[i] += varDataTLen(input[i]);
      }

      if (i < inputNum - 1) {
        //insert the separator
        char *sep = pInputData[0]->pData;
        int32_t ret = concatCopyHelper(sep, output, hasNcharCol, GET_PARAM_TYPE(&pInput[0]), &dataLen);
        if (ret != TSDB_CODE_SUCCESS) {
          return ret;
        }
      }
    }
    varDataSetLen(output, dataLen);
    colDataAppend(pOutputData, k, output, false);
    output += varDataTLen(output);
  }

  pOutput->numOfRows = numOfRows;
  taosMemoryFree(input);
  taosMemoryFree(outputBuf);
  taosMemoryFree(pInputData);

  return TSDB_CODE_SUCCESS;
}

static int32_t doCaseConvFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput, _conv_fn convFn) {
  int32_t type = GET_PARAM_TYPE(pInput);
  if (inputNum != 1 || !IS_VAR_DATA_TYPE(type)) {
    return TSDB_CODE_FAILED;
  }

  SColumnInfoData *pInputData  = pInput->columnData;
  SColumnInfoData *pOutputData = pOutput->columnData;

  char *input  = pInputData->pData + pInputData->varmeta.offset[0];
  char *output = NULL;

  int32_t outputLen = pInputData->varmeta.length;
  char *outputBuf = taosMemoryCalloc(outputLen, 1);
  output = outputBuf;

  for (int32_t i = 0; i < pInput->numOfRows; ++i) {
    if (colDataIsNull_s(pInputData, i)) {
      colDataAppendNULL(pOutputData, i);
      continue;
    }

    int32_t len = varDataLen(input);
    if (type == TSDB_DATA_TYPE_VARCHAR) {
      for (int32_t j = 0; j < len; ++j) {
        *(varDataVal(output) + j) = convFn(*(varDataVal(input) + j));
      }
    } else { //NCHAR
      for (int32_t j = 0; j < len / TSDB_NCHAR_SIZE; ++j) {
        *((uint32_t *)varDataVal(output) + j) = convFn(*((uint32_t *)varDataVal(input) + j));
      }
    }
    varDataSetLen(output, len);
    colDataAppend(pOutputData, i, output, false);
    input += varDataTLen(input);
    output += varDataTLen(output);
  }

  pOutput->numOfRows = pInput->numOfRows;
  taosMemoryFree(outputBuf);

  return TSDB_CODE_SUCCESS;
}


static int32_t doTrimFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput, _trim_fn trimFn) {
  int32_t type = GET_PARAM_TYPE(pInput);
  if (inputNum != 1 || !IS_VAR_DATA_TYPE(type)) {
    return TSDB_CODE_FAILED;
  }

  SColumnInfoData *pInputData  = pInput->columnData;
  SColumnInfoData *pOutputData = pOutput->columnData;

  char *input  = pInputData->pData + pInputData->varmeta.offset[0];
  char *output = NULL;

  int32_t outputLen = pInputData->varmeta.length;
  char *outputBuf = taosMemoryCalloc(outputLen, 1);
  output = outputBuf;

  for (int32_t i = 0; i < pInput->numOfRows; ++i) {
    if (colDataIsNull_s(pInputData, i)) {
      colDataAppendNULL(pOutputData, i);
      continue;
    }

    int32_t len = varDataLen(input);
    int32_t charLen = (type == TSDB_DATA_TYPE_VARCHAR) ? len : len / TSDB_NCHAR_SIZE;
    trimFn(input, output, type, charLen);

    varDataSetLen(output, len);
    colDataAppend(pOutputData, i, output, false);
    input += varDataTLen(input);
    output += varDataTLen(output);
  }

  pOutput->numOfRows = pInput->numOfRows;
  taosMemoryFree(outputBuf);

  return TSDB_CODE_SUCCESS;
}

int32_t substrFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  if (inputNum != 2 && inputNum!= 3) {
    return TSDB_CODE_FAILED;
  }

  int32_t subPos = 0;
  GET_TYPED_DATA(subPos, int32_t, GET_PARAM_TYPE(&pInput[1]), pInput[1].columnData->pData);
  if (subPos == 0) { //subPos needs to be positive or negative values;
    return TSDB_CODE_FAILED;
  }

  int32_t subLen = INT16_MAX;
  if (inputNum == 3) {
    GET_TYPED_DATA(subLen, int32_t, GET_PARAM_TYPE(&pInput[2]), pInput[2].columnData->pData);
    if (subLen < 0) { //subLen cannot be negative
      return TSDB_CODE_FAILED;
    }
    subLen = (GET_PARAM_TYPE(pInput) == TSDB_DATA_TYPE_VARCHAR) ? subLen : subLen * TSDB_NCHAR_SIZE;
  }

  SColumnInfoData *pInputData  = pInput->columnData;
  SColumnInfoData *pOutputData = pOutput->columnData;

  char *input  = pInputData->pData + pInputData->varmeta.offset[0];
  char *output = NULL;

  int32_t outputLen = pInputData->varmeta.length * pInput->numOfRows;
  char *outputBuf = taosMemoryCalloc(outputLen, 1);
  output = outputBuf;

  for (int32_t i = 0; i < pInput->numOfRows; ++i) {
    if (colDataIsNull_s(pInputData, i)) {
      colDataAppendNULL(pOutputData, i);
      continue;
    }

    int32_t len = varDataLen(input);
    int32_t startPosBytes;

    if (subPos > 0) {
      startPosBytes = (GET_PARAM_TYPE(pInput) == TSDB_DATA_TYPE_VARCHAR) ? subPos - 1 : (subPos - 1) * TSDB_NCHAR_SIZE;
      startPosBytes = MIN(startPosBytes, len);
    } else {
      startPosBytes = (GET_PARAM_TYPE(pInput) == TSDB_DATA_TYPE_VARCHAR) ? len + subPos : len + subPos * TSDB_NCHAR_SIZE;
      startPosBytes = MAX(startPosBytes, 0);
    }

    int32_t resLen = MIN(subLen, len - startPosBytes);
    if (resLen > 0) {
      memcpy(varDataVal(output), varDataVal(input) + startPosBytes, resLen);
    }

    varDataSetLen(output, resLen);
    colDataAppend(pOutputData, i , output, false);
    input += varDataTLen(input);
    output += varDataTLen(output);
  }

  pOutput->numOfRows = pInput->numOfRows;
  taosMemoryFree(outputBuf);

  return TSDB_CODE_SUCCESS;
}

int32_t castFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  if (inputNum!= 3) {
    return TSDB_CODE_FAILED;
  }

  int16_t inputType  = pInput[0].columnData->info.type;
  int16_t outputType = *(int16_t *)pInput[1].columnData->pData;
  if (outputType != TSDB_DATA_TYPE_BIGINT && outputType != TSDB_DATA_TYPE_UBIGINT &&
      outputType != TSDB_DATA_TYPE_VARCHAR && outputType != TSDB_DATA_TYPE_NCHAR &&
      outputType != TSDB_DATA_TYPE_TIMESTAMP) {
    return TSDB_CODE_FAILED;
  }
  int64_t outputLen = *(int64_t *)pInput[2].columnData->pData;

  char *input = NULL;
  char *outputBuf = taosMemoryCalloc(outputLen * pInput[0].numOfRows, 1);
  char *output = outputBuf;
  if (IS_VAR_DATA_TYPE(inputType)) {
    input = pInput[0].columnData->pData + pInput[0].columnData->varmeta.offset[0];
  } else {
    input = pInput[0].columnData->pData;
  }

  for (int32_t i = 0; i < pInput[0].numOfRows; ++i) {
    if (colDataIsNull_s(pInput[0].columnData, i)) {
      colDataAppendNULL(pOutput->columnData, i);
      continue;
    }

    switch(outputType) {
      case TSDB_DATA_TYPE_BIGINT: {
        if (inputType == TSDB_DATA_TYPE_BINARY) {
          memcpy(output, varDataVal(input), varDataLen(input));
          *(int64_t *)output = strtoll(output, NULL, 10);
        } else if (inputType == TSDB_DATA_TYPE_NCHAR) {
          char *newBuf = taosMemoryCalloc(1, outputLen * TSDB_NCHAR_SIZE + 1);
          int32_t len  = taosUcs4ToMbs((TdUcs4 *)varDataVal(input), varDataLen(input), newBuf);
          if (len < 0) {
            taosMemoryFree(newBuf);
            return TSDB_CODE_FAILED;
          }
          newBuf[len] = 0;
          *(int64_t *)output = strtoll(newBuf, NULL, 10);
          taosMemoryFree(newBuf);
        } else {
          GET_TYPED_DATA(*(int64_t *)output, int64_t, inputType, input);
        }
        break;
      }
      case TSDB_DATA_TYPE_UBIGINT: {
        if (inputType == TSDB_DATA_TYPE_BINARY) {
          memcpy(output, varDataVal(input), varDataLen(input));
          *(uint64_t *)output = strtoull(output, NULL, 10);
        } else if (inputType == TSDB_DATA_TYPE_NCHAR) {
          char *newBuf = taosMemoryCalloc(1, outputLen * TSDB_NCHAR_SIZE + 1);
          int32_t len = taosUcs4ToMbs((TdUcs4 *)varDataVal(input), varDataLen(input), newBuf);
          if (len < 0) {
            taosMemoryFree(newBuf);
            return TSDB_CODE_FAILED;
          }
          newBuf[len] = 0;
          *(uint64_t *)output = strtoull(newBuf, NULL, 10);
          taosMemoryFree(newBuf);
        } else {
          GET_TYPED_DATA(*(uint64_t *)output, uint64_t, inputType, input);
        }
        break;
      }
      case TSDB_DATA_TYPE_TIMESTAMP: {
        if (inputType == TSDB_DATA_TYPE_BINARY || inputType == TSDB_DATA_TYPE_NCHAR) {
          //not support
          return TSDB_CODE_FAILED;
        } else {
          GET_TYPED_DATA(*(int64_t *)output, int64_t, inputType, input);
        }
        break;
      }
      case TSDB_DATA_TYPE_BINARY: {
        if (inputType == TSDB_DATA_TYPE_BOOL) {
          int32_t len = sprintf(varDataVal(output), "%.*s", (int32_t)(outputLen - VARSTR_HEADER_SIZE), *(int8_t *)input ? "true" : "false");
          varDataSetLen(output, len);
        } else if (inputType == TSDB_DATA_TYPE_BINARY) {
          int32_t len = sprintf(varDataVal(output), "%.*s", (int32_t)(outputLen - VARSTR_HEADER_SIZE), varDataVal(input));
          varDataSetLen(output, len);
        } else if (inputType == TSDB_DATA_TYPE_BINARY || inputType == TSDB_DATA_TYPE_NCHAR) {
          //not support
          return TSDB_CODE_FAILED;
        } else {
          char tmp[400] = {0};
          NUM_TO_STRING(inputType, input, sizeof(tmp), tmp);
          int32_t len = (int32_t)strlen(tmp);
          len = (outputLen - VARSTR_HEADER_SIZE) > len ? len : (outputLen - VARSTR_HEADER_SIZE);
          memcpy(varDataVal(output), tmp, len);
          varDataSetLen(output, len);
        }
        break;
      }
      case TSDB_DATA_TYPE_NCHAR: {
        int32_t outputCharLen = (outputLen - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE;
        if (inputType == TSDB_DATA_TYPE_BOOL) {
          char tmp[8] = {0};
          int32_t len = sprintf(tmp, "%.*s", outputCharLen, *(int8_t *)input ? "true" : "false" );
          bool ret = taosMbsToUcs4(tmp, len, (TdUcs4 *)varDataVal(output), outputLen - VARSTR_HEADER_SIZE, &len);
          if (!ret) {
            return TSDB_CODE_FAILED;
          }
          varDataSetLen(output, len);
        } else if (inputType == TSDB_DATA_TYPE_BINARY) {
          int32_t len = outputCharLen > varDataLen(input) ? varDataLen(input) : outputCharLen;
          bool ret = taosMbsToUcs4(input + VARSTR_HEADER_SIZE, len, (TdUcs4 *)varDataVal(output), outputLen - VARSTR_HEADER_SIZE, &len);
          if (!ret) {
            return TSDB_CODE_FAILED;
          }
          varDataSetLen(output, len);
        } else if (inputType == TSDB_DATA_TYPE_NCHAR) {
          int32_t len = MIN(outputLen, varDataLen(input) + VARSTR_HEADER_SIZE);
          memcpy(output, input, len);
          varDataSetLen(output, len - VARSTR_HEADER_SIZE);
        } else {
          char tmp[400] = {0};
          NUM_TO_STRING(inputType, input, sizeof(tmp), tmp);
          int32_t len = (int32_t)strlen(tmp);
          len = outputCharLen > len ? len : outputCharLen;
          bool ret = taosMbsToUcs4(tmp, len, (TdUcs4 *)varDataVal(output), outputLen - VARSTR_HEADER_SIZE, &len);
          if (!ret) {
            return TSDB_CODE_FAILED;
          }
          varDataSetLen(output, len);
        }
        break;
      }
      default: {
        return TSDB_CODE_FAILED;
      }
    }

    colDataAppend(pOutput->columnData, i, output, false);
    if (IS_VAR_DATA_TYPE(inputType)) {
      input  += varDataTLen(input);
    } else {
      input  += tDataTypes[inputType].bytes;
    }
    if (IS_VAR_DATA_TYPE(outputType)) {
      output += varDataTLen(output);
    } else {
      output += tDataTypes[outputType].bytes;
    }
  }

  pOutput->numOfRows = pInput->numOfRows;
  taosMemoryFree(outputBuf);
  return TSDB_CODE_SUCCESS;
}

int32_t toISO8601Function(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  int32_t type = GET_PARAM_TYPE(pInput);
  if (type != TSDB_DATA_TYPE_BIGINT && type != TSDB_DATA_TYPE_TIMESTAMP) {
    return TSDB_CODE_FAILED;
  }

  if (inputNum != 1) {
    return TSDB_CODE_FAILED;
  }

  char *input  = pInput[0].columnData->pData;
  for (int32_t i = 0; i < pInput[0].numOfRows; ++i) {
    if (colDataIsNull_s(pInput[0].columnData, i)) {
      colDataAppendNULL(pOutput->columnData, i);
      continue;
    }

    char fraction[20] = {0};
    bool hasFraction = false;
    NUM_TO_STRING(type, input, sizeof(fraction), fraction);
    int32_t tsDigits = (int32_t)strlen(fraction);

    char buf[64] = {0};
    int64_t timeVal;
    GET_TYPED_DATA(timeVal, int64_t, type, input);
    if (tsDigits > TSDB_TIME_PRECISION_SEC_DIGITS) {
      if (tsDigits == TSDB_TIME_PRECISION_MILLI_DIGITS) {
        timeVal = timeVal / 1000;
      } else if (tsDigits == TSDB_TIME_PRECISION_MICRO_DIGITS) {
        timeVal = timeVal / (1000 * 1000);
      } else if (tsDigits == TSDB_TIME_PRECISION_NANO_DIGITS) {
        timeVal = timeVal / (1000 * 1000 * 1000);
      } else {
        assert(0);
      }
      hasFraction = true;
      memmove(fraction, fraction + TSDB_TIME_PRECISION_SEC_DIGITS, TSDB_TIME_PRECISION_SEC_DIGITS);
    }

    struct tm *tmInfo = localtime((const time_t *)&timeVal);
    strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%S%z", tmInfo);
    int32_t len = (int32_t)strlen(buf);

    if (hasFraction) {
      int32_t fracLen = (int32_t)strlen(fraction) + 1;
      char *tzInfo = strchr(buf, '+');
      if (tzInfo) {
        memmove(tzInfo + fracLen, tzInfo, strlen(tzInfo));
      } else {
        tzInfo = strchr(buf, '-');
        memmove(tzInfo + fracLen, tzInfo, strlen(tzInfo));
      }

      char tmp[32];
      sprintf(tmp, ".%s", fraction);
      memcpy(tzInfo, tmp, fracLen);
      len += fracLen;
    }

    memmove(buf + VARSTR_HEADER_SIZE, buf, len);
    varDataSetLen(buf, len);

    colDataAppend(pOutput->columnData, i, buf, false);
    input   += tDataTypes[type].bytes;
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
  return doScalarFunctionUnique2(pInput, inputNum, pOutput, tlog);
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
  return doCaseConvFunction(pInput, inputNum, pOutput, tolower);
}

int32_t upperFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  return doCaseConvFunction(pInput, inputNum, pOutput, toupper);
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

#if 0
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
#endif

bool getTimePseudoFuncEnv(SFunctionNode* UNUSED_PARAM(pFunc), SFuncExecEnv* pEnv) {
  pEnv->calcMemSize = sizeof(int64_t);
  return true;
}

int32_t qStartTsFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  ASSERT(inputNum == 1);
  colDataAppendInt64(pOutput->columnData, pOutput->numOfRows, (int64_t *)colDataGetData(pInput->columnData, 0));
  return TSDB_CODE_SUCCESS;
}

int32_t qEndTsFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  ASSERT(inputNum == 1);
  colDataAppendInt64(pOutput->columnData, pOutput->numOfRows, (int64_t *)colDataGetData(pInput->columnData, 1));
  return TSDB_CODE_SUCCESS;
}

int32_t winDurFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  ASSERT(inputNum == 1);
  colDataAppendInt64(pOutput->columnData, pOutput->numOfRows, (int64_t *)colDataGetData(pInput->columnData, 2));
  return TSDB_CODE_SUCCESS;
}

int32_t winStartTsFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  ASSERT(inputNum == 1);
  colDataAppendInt64(pOutput->columnData, pOutput->numOfRows, (int64_t*) colDataGetData(pInput->columnData, 3));
  return TSDB_CODE_SUCCESS;
}

int32_t winEndTsFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  ASSERT(inputNum == 1);
  colDataAppendInt64(pOutput->columnData, pOutput->numOfRows, (int64_t*) colDataGetData(pInput->columnData, 4));
  return TSDB_CODE_SUCCESS;
}
