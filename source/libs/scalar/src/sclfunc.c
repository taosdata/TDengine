#include "sclfunc.h"
#include <common/tdatablock.h>
#include "sclInt.h"
#include "sclvector.h"

typedef float (*_float_fn)(float);
typedef double (*_double_fn)(double);
typedef double (*_double_fn_2)(double, double);
typedef int (*_conv_fn)(int);
typedef void (*_trim_fn)(char *, char*, int32_t, int32_t);

double tlog(double v, double base) {
  return log(v) / log(base);
}

/** Math functions **/
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
        out[i] = (in[i] > 0)? in[i] : -in[i];
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
        out[i] = (in[i] > 0)? in[i] : -in[i];
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
        out[i] = (in[i] > 0)? in[i] : -in[i];
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
        out[i] = (in[i] > 0)? in[i] : -in[i];
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
        out[i] = (in[i] > 0)? in[i] : -in[i];
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
        out[i] = (in[i] > 0)? in[i] : -in[i];
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

int32_t doScalarFunctionUnique(SScalarParam *pInput, int32_t inputNum, SScalarParam* pOutput, _double_fn valFn) {
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

int32_t doScalarFunctionUnique2(SScalarParam *pInput, int32_t inputNum, SScalarParam* pOutput, _double_fn_2 valFn) {
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

int32_t doScalarFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam* pOutput, _float_fn f1, _double_fn d1) {
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
int32_t lengthFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  int32_t type = GET_PARAM_TYPE(pInput);
  if (inputNum != 1 || !IS_VAR_DATA_TYPE(type)) {
    return TSDB_CODE_FAILED;
  }

  SColumnInfoData *pInputData  = pInput->columnData;
  SColumnInfoData *pOutputData = pOutput->columnData;

  char **in = (char **)pInputData->pData;
  int16_t *out = (int16_t *)pOutputData->pData;

  for (int32_t i = 0; i < pInput->numOfRows; ++i) {
    if (colDataIsNull_f(pInputData->nullbitmap, i)) {
      colDataSetNull_f(pOutputData->nullbitmap, i);
      continue;
    }

    out[i] = varDataLen(in[i]);
  }

  pOutput->numOfRows = pInput->numOfRows;
  return TSDB_CODE_SUCCESS;
}

int32_t charLengthFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  int32_t type = GET_PARAM_TYPE(pInput);
  if (inputNum != 1 || !IS_VAR_DATA_TYPE(type)) {
    return TSDB_CODE_FAILED;
  }

  SColumnInfoData *pInputData  = pInput->columnData;
  SColumnInfoData *pOutputData = pOutput->columnData;

  char **in = (char **)pInputData->pData;
  int16_t *out = (int16_t *)pOutputData->pData;

  for (int32_t i = 0; i < pInput->numOfRows; ++i) {
    if (colDataIsNull_f(pInputData->nullbitmap, i)) {
      colDataSetNull_f(pOutputData->nullbitmap, i);
      continue;
    }

    if (type == TSDB_DATA_TYPE_VARCHAR) {
      out[i] = varDataLen(in[i]);
    } else { //NCHAR
      out[i] = varDataLen(in[i]) / TSDB_NCHAR_SIZE;
    }
  }

  pOutput->numOfRows = pInput->numOfRows;
  return TSDB_CODE_SUCCESS;
}

int32_t concatFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  if (inputNum < 2 || inputNum > 8) { // concat accpet 2-8 input strings
    return TSDB_CODE_FAILED;
  }

  SColumnInfoData **pInputData = taosMemoryCalloc(inputNum, sizeof(SColumnInfoData *));
  SColumnInfoData *pOutputData = pOutput->columnData;

  for (int32_t i = 0; i < inputNum; ++i) {
    if (!IS_VAR_DATA_TYPE(GET_PARAM_TYPE(&pInput[i])) ||
        GET_PARAM_TYPE(&pInput[i]) != GET_PARAM_TYPE(&pInput[0])) {
      return TSDB_CODE_FAILED;
    }
    pInputData[i] = pInput[i].columnData;
  }

  bool hasNull = false;
  for (int32_t k = 0; k < pInput->numOfRows; ++k) {
    for (int32_t i = 0; i < inputNum; ++i) {
      if (colDataIsNull_f(pInputData[i]->nullbitmap, k)) {
        colDataSetNull_f(pOutputData->nullbitmap, k);
        hasNull = true;
        break;
      }
    }

    if (hasNull) {
      continue;
    }

    char *in  = NULL;
    char *out = pOutputData->pData + k * GET_PARAM_BYTES(pOutput);

    int16_t dataLen = 0;
    for (int32_t i = 0; i < inputNum; ++i) {
      in = pInputData[i]->pData + k * GET_PARAM_BYTES(&pInput[i]);

      memcpy(varDataVal(out) + dataLen, varDataVal(in), varDataLen(in));
      dataLen += varDataLen(in);
    }
    varDataSetLen(out, dataLen);
  }

  pOutput->numOfRows = pInput->numOfRows;
  taosMemoryFree(pInputData);

  return TSDB_CODE_SUCCESS;
}

int32_t concatWsFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  if (inputNum < 3 || inputNum > 9) { // concat accpet 3-9 input strings including the separator
    return TSDB_CODE_FAILED;
  }

  SColumnInfoData **pInputData = taosMemoryCalloc(inputNum, sizeof(SColumnInfoData *));
  SColumnInfoData *pOutputData = pOutput->columnData;

  for (int32_t i = 0; i < inputNum; ++i) {
    if (!IS_VAR_DATA_TYPE(GET_PARAM_TYPE(&pInput[i])) ||
        GET_PARAM_TYPE(&pInput[i]) != GET_PARAM_TYPE(&pInput[0])) {
      return TSDB_CODE_FAILED;
    }
    pInputData[i] = pInput[i].columnData;
  }

  for (int32_t k = 0; k < pInput->numOfRows; ++k) {
    char *sep = pInputData[0]->pData;
    if (colDataIsNull_f(pInputData[0]->nullbitmap, k)) {
      colDataSetNull_f(pOutputData->nullbitmap, k);
      continue;
    }

    char *in  = NULL;
    char *out = pOutputData->pData + k * GET_PARAM_BYTES(pOutput);

    int16_t dataLen = 0;
    for (int32_t i = 1; i < inputNum; ++i) {
      if (colDataIsNull_f(pInputData[i]->nullbitmap, k)) {
        continue;
      }

      in  = pInputData[i]->pData + k * GET_PARAM_BYTES(&pInput[i]);
      memcpy(varDataVal(out) + dataLen, varDataVal(in), varDataLen(in));
      dataLen += varDataLen(in);

      if (i < inputNum - 1) {
        //insert the separator
        memcpy(varDataVal(out) + dataLen, varDataVal(sep), varDataLen(sep));
        dataLen += varDataLen(sep);
      }
    }
    varDataSetLen(out, dataLen);
  }

  pOutput->numOfRows = pInput->numOfRows;
  taosMemoryFree(pInputData);

  return TSDB_CODE_SUCCESS;
}

int32_t doCaseConvFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput, _conv_fn convFn) {
  int32_t type = GET_PARAM_TYPE(pInput);
  if (inputNum != 1 || !IS_VAR_DATA_TYPE(type)) {
    return TSDB_CODE_FAILED;
  }

  SColumnInfoData *pInputData  = pInput->columnData;
  SColumnInfoData *pOutputData = pOutput->columnData;

  for (int32_t i = 0; i < pInput->numOfRows; ++i) {
    if (colDataIsNull_f(pInputData->nullbitmap, i)) {
      colDataSetNull_f(pOutputData->nullbitmap, i);
      continue;
    }

    char *in  = pInputData->pData + i * GET_PARAM_BYTES(pInput);
    char *out = pOutputData->pData + i * GET_PARAM_BYTES(pInput);

    int32_t len = varDataLen(in);
    if (type == TSDB_DATA_TYPE_VARCHAR) {
      for (int32_t j = 0; j < len; ++j) {
        *(varDataVal(out) + j) = convFn(*(varDataVal(in) + j));
      }
    } else { //NCHAR
      for (int32_t j = 0; j < len / TSDB_NCHAR_SIZE; ++j) {
        *((uint32_t *)varDataVal(out) + j) = convFn(*((uint32_t *)varDataVal(in) + j));
      }
    }
    varDataSetLen(out, len);
  }

  pOutput->numOfRows = pInput->numOfRows;

  return TSDB_CODE_SUCCESS;
}

void tltrim(char *input, char *output, int32_t type, int32_t charLen) {
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

void trtrim(char *input, char *output, int32_t type, int32_t charLen) {
  int32_t numOfSpaces = 0;
  if (type == TSDB_DATA_TYPE_VARCHAR) {
    for (int32_t i = charLen - 1; i >= 0; --i) {
      if (!isspace(*(varDataVal(input) + i))) {
        break;
      }
      numOfSpaces++;
    }
  } else { //NCHAR
    for (int32_t i = charLen - 1; i < charLen; ++i) {
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

int32_t doTrimFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput, _trim_fn trimFn) {
  int32_t type = GET_PARAM_TYPE(pInput);
  if (inputNum != 1 || !IS_VAR_DATA_TYPE(type)) {
    return TSDB_CODE_FAILED;
  }

  SColumnInfoData *pInputData  = pInput->columnData;
  SColumnInfoData *pOutputData = pOutput->columnData;

  for (int32_t i = 0; i < pInput->numOfRows; ++i) {
    if (colDataIsNull_f(pInputData->nullbitmap, i)) {
      colDataSetNull_f(pOutputData->nullbitmap, i);
      continue;
    }

    char *in  = pInputData->pData + i * GET_PARAM_BYTES(pInput);
    char *out = pOutputData->pData + i * GET_PARAM_BYTES(pInput);

    int32_t len = varDataLen(in);
    int32_t charLen = (type == TSDB_DATA_TYPE_VARCHAR) ? len : len / TSDB_NCHAR_SIZE;
    trimFn(in, out, type, charLen);
  }

  pOutput->numOfRows = pInput->numOfRows;

  return TSDB_CODE_SUCCESS;
}

int32_t substrFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  if (inputNum != 2 || inputNum!= 3) {
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

  for (int32_t i = 0; i < pOutput->numOfRows; ++i) {
    if (colDataIsNull_f(pInputData->nullbitmap, i)) {
      colDataSetNull_f(pOutputData->nullbitmap, i);
      continue;
    }

    char *in  = pInputData->pData + i * GET_PARAM_BYTES(pInput);
    char *out = pOutputData->pData + i * GET_PARAM_BYTES(pInput);

    int32_t len = varDataLen(in);
    int32_t startPosBytes;

    if (subPos > 0) {
      startPosBytes = (GET_PARAM_TYPE(pInput) == TSDB_DATA_TYPE_VARCHAR) ? subPos - 1 : (subPos - 1) * TSDB_NCHAR_SIZE;
      startPosBytes = MIN(startPosBytes, len);
    } else {
      startPosBytes = (GET_PARAM_TYPE(pInput) == TSDB_DATA_TYPE_VARCHAR) ? len + subPos : len + subPos * TSDB_NCHAR_SIZE;
      startPosBytes = MAX(startPosBytes, 0);
    }

    subLen = MIN(subLen, len - startPosBytes);
    if (subLen > 0) {
      memcpy(varDataVal(out), varDataVal(in) + startPosBytes, subLen);
    }

    varDataSetLen(out, subLen);
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

