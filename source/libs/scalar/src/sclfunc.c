#include "cJSON.h"
#include "function.h"
#include "scalar.h"
#include "sclInt.h"
#include "sclvector.h"
#include "tdatablock.h"
#include "tjson.h"
#include "ttime.h"

typedef float (*_float_fn)(float);
typedef float (*_float_fn_2)(float, float);
typedef double (*_double_fn)(double);
typedef double (*_double_fn_2)(double, double);
typedef int (*_conv_fn)(int);
typedef void (*_trim_space_fn)(char *, char *, int32_t, int32_t);
typedef int32_t (*_trim_fn)(char *, char *, char *, int32_t, int32_t);
typedef int32_t (*_len_fn)(char *, int32_t, VarDataLenT *);

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

static double degrees(double v) {
  return v * M_1_PI * 180.0;
}

static double radians(double v) {
  return v / 180.0 * M_PI ;
}

int32_t absFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  int32_t          code = TSDB_CODE_SUCCESS;
  int32_t          lino = 0;
  SCL_CHECK_NULL(pInput, code, lino, _return, TSDB_CODE_INVALID_PARA)
  SCL_CHECK_NULL(pOutput, code, lino, _return, TSDB_CODE_INVALID_PARA)
  SCL_CHECK_NULL(pInput->columnData, code, lino, _return, TSDB_CODE_INVALID_PARA)
  SCL_CHECK_NULL(pOutput->columnData, code, lino, _return, TSDB_CODE_INVALID_PARA)
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
      SCL_ERR_RET(colDataAssign(pOutputData, pInputData, pInput->numOfRows, NULL));
    }
  }

  pOutput->numOfRows = pInput->numOfRows;
_return:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

int32_t signFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  int32_t          code = TSDB_CODE_SUCCESS;
  int32_t          lino = 0;
  SCL_CHECK_NULL(pInput, code, lino, _return, TSDB_CODE_INVALID_PARA)
  SCL_CHECK_NULL(pOutput, code, lino, _return, TSDB_CODE_INVALID_PARA)
  SCL_CHECK_NULL(pInput->columnData, code, lino, _return, TSDB_CODE_INVALID_PARA)
  SCL_CHECK_NULL(pOutput->columnData, code, lino, _return, TSDB_CODE_INVALID_PARA)
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
        out[i] = (float)(in[i] < 0.0 ? -1 : (in[i] > 0.0 ? 1 : 0));
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
        out[i] = (double)(in[i] < 0.0 ? -1 : (in[i] > 0.0 ? 1 : 0));
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
        out[i] = (int8_t)(in[i] < 0 ? -1 : (in[i] > 0 ? 1 : 0));
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
        out[i] = (int16_t)(in[i] < 0 ? -1 : (in[i] > 0 ? 1 : 0));
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
        out[i] = (int32_t)(in[i] < 0 ? -1 : (in[i] > 0 ? 1 : 0));
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
        out[i] = (int64_t)(in[i] < 0 ? -1 : (in[i] > 0 ? 1 : 0));
      }
      break;
    }

    case TSDB_DATA_TYPE_UTINYINT: {
      uint8_t *in = (uint8_t *)pInputData->pData;
      uint8_t *out = (uint8_t *)pOutputData->pData;
      for (int32_t i = 0; i < pInput->numOfRows; ++i) {
        if (colDataIsNull_s(pInputData, i)) {
          colDataSetNULL(pOutputData, i);
          continue;
        }
        out[i] = (uint8_t)(in[i] > 0 ? 1 : 0);
      }
      break;
    }
    case TSDB_DATA_TYPE_USMALLINT: {
      uint16_t *in = (uint16_t *)pInputData->pData;
      uint16_t *out = (uint16_t *)pOutputData->pData;
      for (int32_t i = 0; i < pInput->numOfRows; ++i) {
        if (colDataIsNull_s(pInputData, i)) {
          colDataSetNULL(pOutputData, i);
          continue;
        }
        out[i] = (uint16_t)(in[i] > 0 ? 1 : 0);
      }
      break;
    }
    case TSDB_DATA_TYPE_UINT: {
      uint32_t *in = (uint32_t *)pInputData->pData;
      uint32_t *out = (uint32_t *)pOutputData->pData;
      for (int32_t i = 0; i < pInput->numOfRows; ++i) {
        if (colDataIsNull_s(pInputData, i)) {
          colDataSetNULL(pOutputData, i);
          continue;
        }
        out[i] = (uint32_t)(in[i] > 0 ? 1 : 0);
      }
      break;
    }
    case TSDB_DATA_TYPE_UBIGINT: {
      uint64_t *in = (uint64_t *)pInputData->pData;
      uint64_t *out = (uint64_t *)pOutputData->pData;
      for (int32_t i = 0; i < pInput->numOfRows; ++i) {
        if (colDataIsNull_s(pInputData, i)) {
          colDataSetNULL(pOutputData, i);
          continue;
        }
        out[i] = (uint64_t)(in[i] > 0 ? 1 : 0);
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
      SCL_ERR_RET(TSDB_CODE_FUNC_FUNTION_PARA_TYPE);
    }
  }

  pOutput->numOfRows = pInput->numOfRows;
_return:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t doScalarFunctionUnique(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput, _double_fn valFn) {
  int32_t          code = TSDB_CODE_SUCCESS;
  int32_t          lino = 0;
  SCL_CHECK_NULL(pInput, code, lino, _return, TSDB_CODE_INVALID_PARA)
  SCL_CHECK_NULL(pOutput, code, lino, _return, TSDB_CODE_INVALID_PARA)
  SCL_CHECK_NULL(pInput->columnData, code, lino, _return, TSDB_CODE_INVALID_PARA)
  SCL_CHECK_NULL(pOutput->columnData, code, lino, _return, TSDB_CODE_INVALID_PARA)
  SCL_CHECK_NULL(valFn, code, lino, _return, TSDB_CODE_INVALID_PARA)

  int32_t type = GET_PARAM_TYPE(pInput);

  SColumnInfoData *pInputData = pInput->columnData;
  SColumnInfoData *pOutputData = pOutput->columnData;

  _getDoubleValue_fn_t getValueFn;
  SCL_ERR_RET(getVectorDoubleValueFn(type, &getValueFn));

  double *out = (double *)pOutputData->pData;

  for (int32_t i = 0; i < pInput->numOfRows; ++i) {
    if (colDataIsNull_s(pInputData, i) || IS_NULL_TYPE(type)) {
      colDataSetNULL(pOutputData, i);
      continue;
    }
    double tmp = 0;
    SCL_ERR_RET(getValueFn(pInputData->pData, i, &tmp));
    double result = valFn(tmp);
    if (isinf(result) || isnan(result)) {
      colDataSetNULL(pOutputData, i);
    } else {
      out[i] = result;
    }
  }

  pOutput->numOfRows = pInput->numOfRows;
_return:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t doScalarFunctionUnique2(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput,
                                       _double_fn_2 valFn) {
  int32_t          code = TSDB_CODE_SUCCESS;
  int32_t          lino = 0;
  SCL_CHECK_NULL(pInput, code, lino, _return, TSDB_CODE_INVALID_PARA)
  SCL_CHECK_NULL(pOutput, code, lino, _return, TSDB_CODE_INVALID_PARA)
  SCL_CHECK_NULL(pOutput->columnData, code, lino, _return, TSDB_CODE_INVALID_PARA)
  SCL_CHECK_NULL(valFn, code, lino, _return, TSDB_CODE_INVALID_PARA)

  SColumnInfoData     *pInputData[2];
  SColumnInfoData     *pOutputData = pOutput->columnData;
  _getDoubleValue_fn_t getValueFn[2];

  for (int32_t i = 0; i < inputNum; ++i) {
    SCL_CHECK_NULL(pInput[i].columnData, code, lino, _return, TSDB_CODE_INVALID_PARA)
    pInputData[i] = pInput[i].columnData;
    SCL_ERR_RET(getVectorDoubleValueFn(GET_PARAM_TYPE(&pInput[i]), &getValueFn[i]));
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
      double val1 = 0;
      double val2 = 0;
      SCL_ERR_RET(getValueFn[0](pInputData[0]->pData, i, &val1));
      SCL_ERR_RET(getValueFn[1](pInputData[1]->pData, i, &val2));
      result = valFn(val1, val2);
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
        double val1 = 0;
        double val2 = 0;
        SCL_ERR_RET(getValueFn[0](pInputData[0]->pData, 0, &val1));
        SCL_ERR_RET(getValueFn[1](pInputData[1]->pData, i, &val2));
        result = valFn(val1, val2);
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
        double val1 = 0;
        double val2 = 0;
        SCL_ERR_RET(getValueFn[0](pInputData[0]->pData, i, &val1));
        SCL_ERR_RET(getValueFn[1](pInputData[1]->pData, 0, &val2));
        result = valFn(val1, val2);
        if (isinf(result) || isnan(result)) {
          colDataSetNULL(pOutputData, i);
          continue;
        }

        out[i] = result;
      }
    }
  }

  pOutput->numOfRows = numOfRows;
_return:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

static int32_t doScalarFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput, _float_fn f1,
                                _double_fn d1) {
  int32_t          code = TSDB_CODE_SUCCESS;
  int32_t          lino = 0;
  SCL_CHECK_NULL(pInput, code, lino, _return, TSDB_CODE_INVALID_PARA)
  SCL_CHECK_NULL(pOutput, code, lino, _return, TSDB_CODE_INVALID_PARA)
  SCL_CHECK_NULL(pOutput->columnData, code, lino, _return, TSDB_CODE_INVALID_PARA)
  SCL_CHECK_NULL(pInput->columnData, code, lino, _return, TSDB_CODE_INVALID_PARA)
  SCL_CHECK_NULL(f1, code, lino, _return, TSDB_CODE_INVALID_PARA)
  SCL_CHECK_NULL(d1, code, lino, _return, TSDB_CODE_INVALID_PARA)

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
        out[i] = f1(in[i]) + 0;
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
        out[i] = d1(in[i]) + 0;
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
      SCL_ERR_RET(colDataAssign(pOutputData, pInputData, pInput->numOfRows, NULL));
    }
  }

  pOutput->numOfRows = pInput->numOfRows;
_return:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

/** String functions **/
static int32_t tlength(char *input, int32_t type, VarDataLenT *len) {
  int32_t          code = TSDB_CODE_SUCCESS;
  int32_t          lino = 0;
  SCL_CHECK_NULL(input, code, lino, _return, TSDB_CODE_INVALID_PARA)
  SCL_CHECK_NULL(len, code, lino, _return, TSDB_CODE_INVALID_PARA)
  *len = varDataLen(input);
_return:
  if (code != TSDB_CODE_SUCCESS) {
    qError("%s failed at line %d since %s", __func__, lino, tstrerror(code));
  }
  return code;
}

uint8_t getCharLen(const unsigned char *str) {
  if (strcasecmp(tsCharset, "UTF-8") != 0) {
    return 1;
  }
  if ((str[0] & 0x80) == 0) {
    return 1;
  } else if ((str[0] & 0xE0) == 0xC0) {
    return 2;
  } else if ((str[0] & 0xF0) == 0xE0) {
    return 3;
  } else if ((str[0] & 0xF8) == 0xF0) {
    return 4;
  } else {
    return 1;
  }
}

static int32_t tcharlength(char *input, int32_t type, VarDataLenT *len) {
  if (type == TSDB_DATA_TYPE_VARCHAR) {
    // calculate the number of characters in the string considering the multi-byte character
    char *str = varDataVal(input);
    VarDataLenT strLen = 0;
    VarDataLenT pos = 0;
    while(pos < varDataLen(input)) {
      strLen++;
      pos += getCharLen((unsigned char *)(str + pos));
    }
    *len = strLen;
    return TSDB_CODE_SUCCESS;
  } else if (type == TSDB_DATA_TYPE_GEOMETRY) {
    *len = varDataLen(input);
  } else {  // NCHAR
    *len = varDataLen(input) / TSDB_NCHAR_SIZE;
  }
  return TSDB_CODE_SUCCESS;
}

static void tltrimspace(char *input, char *output, int32_t type, int32_t charLen) {
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
    (void)memcpy(varDataVal(output), varDataVal(input) + numOfSpaces, resLen);
  } else {
    resLen = (charLen - numOfSpaces) * TSDB_NCHAR_SIZE;
    (void)memcpy(varDataVal(output), varDataVal(input) + numOfSpaces * TSDB_NCHAR_SIZE, resLen);
  }

  varDataSetLen(output, resLen);
}

static void tlrtrimspace(char *input, char *output, int32_t type, int32_t charLen) {
  int32_t numOfLeftSpaces = 0;
  int32_t numOfRightSpaces = 0;
  if (type == TSDB_DATA_TYPE_VARCHAR) {
    for (int32_t i = 0; i < charLen; ++i) {
      if (!isspace(*(varDataVal(input) + i))) {
        break;
      }
      numOfLeftSpaces++;
    }
  } else {  // NCHAR
    for (int32_t i = 0; i < charLen; ++i) {
      if (!iswspace(*((uint32_t *)varDataVal(input) + i))) {
        break;
      }
      numOfLeftSpaces++;
    }
  }

  if (type == TSDB_DATA_TYPE_VARCHAR) {
    for (int32_t i = charLen - 1; i >= 0; --i) {
      if (!isspace(*(varDataVal(input) + i))) {
        break;
      }
      numOfRightSpaces++;
    }
  } else {  // NCHAR
    for (int32_t i = charLen - 1; i >= 0; --i) {
      if (!iswspace(*((uint32_t *)varDataVal(input) + i))) {
        break;
      }
      numOfRightSpaces++;
    }
  }

  int32_t resLen;
  if (type == TSDB_DATA_TYPE_VARCHAR) {
    resLen = charLen - (numOfLeftSpaces + numOfRightSpaces);
    (void)memcpy(varDataVal(output), varDataVal(input) + numOfLeftSpaces, resLen);
  } else {
    resLen = (charLen - (numOfLeftSpaces + numOfRightSpaces)) * TSDB_NCHAR_SIZE;
    (void)memcpy(varDataVal(output), varDataVal(input) + numOfLeftSpaces * TSDB_NCHAR_SIZE, resLen);
  }

  varDataSetLen(output, resLen);
}

static bool isCharStart(char c) {
  return strcasecmp(tsCharset, "UTF-8") == 0 ? ((c & 0xC0) != 0x80) : true;
}

static int32_t trimHelper(char *orgStr, char* remStr, int32_t orgLen, int32_t remLen, bool trimLeft, bool isNchar) {
  if (trimLeft) {
    int32_t pos = 0;
    for (int32_t i = 0; i < orgLen; i += remLen) {
      if (memcmp(orgStr + i, remStr, remLen) == 0) {
        if (isCharStart(orgStr[i + remLen]) || isNchar) {
          pos = i + remLen;
          continue;
        } else {
          return pos;
        }
      } else {
        return pos;
      }
    }
    return pos;
  } else {
    int32_t pos = orgLen;
    for (int32_t i = orgLen - remLen; i >= 0; i -= remLen) {
      if (memcmp(orgStr + i, remStr, remLen) == 0) {
        if (isCharStart(orgStr[i]) || isNchar) {
          pos = i;
          continue;
        } else {
          return pos;
        }
      } else {
        return pos;
      }
    }
    return pos;
  }
}

static int32_t convVarcharToNchar(char *input, char **output, int32_t inputLen, int32_t *outputLen) {
  *output = taosMemoryCalloc(inputLen * TSDB_NCHAR_SIZE, 1);
  if (NULL == *output) {
    return terrno;
  }
  bool ret = taosMbsToUcs4(input, inputLen, (TdUcs4 *)*output, inputLen * TSDB_NCHAR_SIZE, outputLen);
  if (!ret) {
    taosMemoryFreeClear(*output);
    return TSDB_CODE_SCALAR_CONVERT_ERROR;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t convNcharToVarchar(char *input, char **output, int32_t inputLen, int32_t *outputLen) {
  *output = taosMemoryCalloc(inputLen, 1);
  if (NULL == *output) {
    return terrno;
  }
  *outputLen = taosUcs4ToMbs((TdUcs4 *)input, inputLen, *output);
  if (*outputLen < 0) {
    taosMemoryFreeClear(*output);
    return TSDB_CODE_SCALAR_CONVERT_ERROR;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t convBetweenNcharAndVarchar(char *input, char **output, int32_t inputLen, int32_t *outputLen, int32_t wantType) {
  if (wantType == TSDB_DATA_TYPE_NCHAR) {
    return convVarcharToNchar(input, output, inputLen, outputLen);
  } else {
    return convNcharToVarchar(input, output, inputLen, outputLen);
  }
}
static int32_t tltrim(char *input, char *remInput, char *output, int32_t inputType, int32_t remType) {
  int32_t orgLen = varDataLen(input);
  char   *orgStr = varDataVal(input);
  int32_t remLen = varDataLen(remInput);
  char   *remStr = varDataVal(remInput);
  if (orgLen == 0) {
    varDataSetLen(output, 0);
    return TSDB_CODE_SUCCESS;
  }
  int32_t pos = 0;

  bool    needFree = false;
  if (inputType != remType) {
    SCL_ERR_RET(convBetweenNcharAndVarchar(varDataVal(remInput), &remStr, varDataLen(remInput), &remLen, inputType));
    needFree = true;
  }

  if (remLen == 0 || remLen > orgLen) {
    (void)memcpy(varDataVal(output), orgStr, orgLen);
    varDataSetLen(output, orgLen);
    return TSDB_CODE_SUCCESS;
  }

  pos = trimHelper(orgStr, remStr, orgLen, remLen, true, inputType == TSDB_DATA_TYPE_NCHAR);

  if (needFree) {
    taosMemoryFreeClear(remStr);
  }

  int32_t resLen = orgLen - pos;
  (void)memcpy(varDataVal(output), orgStr + pos, resLen);
  varDataSetLen(output, resLen);
  return TSDB_CODE_SUCCESS;
}

static void trtrimspace(char *input, char *output, int32_t type, int32_t charLen) {
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
  (void)memcpy(varDataVal(output), varDataVal(input), resLen);

  varDataSetLen(output, resLen);
}

static int32_t trtrim(char *input, char *remInput, char *output, int32_t inputType, int32_t remType) {
  int32_t orgLen = varDataLen(input);
  char   *orgStr = varDataVal(input);
  int32_t remLen = varDataLen(remInput);
  char   *remStr = varDataVal(remInput);
  if (orgLen == 0) {
    varDataSetLen(output, 0);
    return TSDB_CODE_SUCCESS;
  }
  int32_t pos = 0;
  bool    needFree = false;

  if (inputType != remType) {
    SCL_ERR_RET(convBetweenNcharAndVarchar(varDataVal(remInput), &remStr, varDataLen(remInput), &remLen, inputType));
    needFree = true;
  }

  if (remLen == 0 || remLen > orgLen) {
    (void)memcpy(varDataVal(output), orgStr, orgLen);
    varDataSetLen(output, orgLen);
    return TSDB_CODE_SUCCESS;
  }

  pos = trimHelper(orgStr, remStr, orgLen, remLen, false, inputType == TSDB_DATA_TYPE_NCHAR);

  if (needFree) {
    taosMemoryFreeClear(remStr);
  }

  (void)memcpy(varDataVal(output), orgStr, pos);
  varDataSetLen(output, pos);
  return TSDB_CODE_SUCCESS;
}

static int32_t tlrtrim(char *input, char *remInput, char *output, int32_t inputType, int32_t remType) {
  int32_t orgLen = varDataLen(input);
  char   *orgStr = varDataVal(input);
  int32_t remLen = varDataLen(remInput);
  char   *remStr = varDataVal(remInput);
  if (orgLen == 0) {
    varDataSetLen(output, 0);
    return TSDB_CODE_SUCCESS;
  }

  bool    needFree = false;

  if (inputType != remType) {
    SCL_ERR_RET(convBetweenNcharAndVarchar(varDataVal(remInput), &remStr, varDataLen(remInput), &remLen, inputType));
    needFree = true;
  }

  if (remLen == 0 || remLen > orgLen) {
    (void)memcpy(varDataVal(output), orgStr, orgLen);
    varDataSetLen(output, orgLen);
    if (needFree) {
      taosMemoryFreeClear(remStr);
    }
    return TSDB_CODE_SUCCESS;
  }

  int32_t leftPos = trimHelper(orgStr, remStr, orgLen, remLen, true, inputType == TSDB_DATA_TYPE_NCHAR);
  int32_t rightPos = trimHelper(orgStr, remStr, orgLen, remLen, false, inputType == TSDB_DATA_TYPE_NCHAR);

  if (needFree) {
    taosMemoryFreeClear(remStr);
  }

  if (leftPos >= rightPos) {
    varDataSetLen(output, 0);
    return TSDB_CODE_SUCCESS;
  }
  (void)memcpy(varDataVal(output), orgStr + leftPos, rightPos - leftPos);
  varDataSetLen(output, rightPos - leftPos);
  return TSDB_CODE_SUCCESS;
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
    SCL_ERR_RET(lenFn(in, type, (VarDataLenT *)&(out[i])));
  }

  pOutput->numOfRows = pInput->numOfRows;
  return TSDB_CODE_SUCCESS;
}

static int32_t concatCopyHelper(const char *input, char *output, bool hasNchar, int32_t type, VarDataLenT *dataLen) {
  if (hasNchar && type == TSDB_DATA_TYPE_VARCHAR) {
    TdUcs4 *newBuf = taosMemoryCalloc((varDataLen(input) + 1) * TSDB_NCHAR_SIZE, 1);
    if (NULL == newBuf) {
      return terrno;
    }
    int32_t len = varDataLen(input);
    bool    ret = taosMbsToUcs4(varDataVal(input), len, newBuf, (varDataLen(input) + 1) * TSDB_NCHAR_SIZE, &len);
    if (!ret) {
      taosMemoryFreeClear(newBuf);
      return TSDB_CODE_SCALAR_CONVERT_ERROR;
    }
    (void)memcpy(varDataVal(output) + *dataLen, newBuf, len);
    *dataLen += len;
    taosMemoryFreeClear(newBuf);
  } else {
    (void)memcpy(varDataVal(output) + *dataLen, varDataVal(input), varDataLen(input));
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
  int32_t           code = TSDB_CODE_SUCCESS;
  SColumnInfoData **pInputData = taosMemoryCalloc(inputNum, sizeof(SColumnInfoData *));
  SColumnInfoData  *pOutputData = pOutput->columnData;
  char            **input = taosMemoryCalloc(inputNum, POINTER_BYTES);
  char             *outputBuf = NULL;

  if (NULL == pInputData) {
    SCL_ERR_JRET(terrno);
  }
  if (NULL == input) {
    SCL_ERR_JRET(terrno);
  }

  int32_t inputLen = 0;
  int32_t numOfRows = 0;
  bool    hasNchar = (GET_PARAM_TYPE(pOutput) == TSDB_DATA_TYPE_NCHAR) ? true : false;
  for (int32_t i = 0; i < inputNum; ++i) {
    numOfRows = TMAX(pInput[i].numOfRows, numOfRows);
  }
  int32_t outputLen = VARSTR_HEADER_SIZE;
  for (int32_t i = 0; i < inputNum; ++i) {
    if (IS_NULL_TYPE(GET_PARAM_TYPE(&pInput[i]))) {
      colDataSetNNULL(pOutputData, 0, numOfRows);
      pOutput->numOfRows = numOfRows;
      goto _return;
    }
    pInputData[i] = pInput[i].columnData;
    int32_t factor = 1;
    if (hasNchar && (GET_PARAM_TYPE(&pInput[i]) == TSDB_DATA_TYPE_VARCHAR)) {
      factor = TSDB_NCHAR_SIZE;
    }
    outputLen += pInputData[i]->info.bytes * factor;
  }

  outputBuf = taosMemoryCalloc(outputLen, 1);
  if (NULL == outputBuf) {
    SCL_ERR_JRET(terrno);
  }

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
    char       *output = outputBuf;
    for (int32_t i = 0; i < inputNum; ++i) {
      int32_t rowIdx = (pInput[i].numOfRows == 1) ? 0 : k;
      input[i] = colDataGetData(pInputData[i], rowIdx);

      SCL_ERR_JRET(concatCopyHelper(input[i], output, hasNchar, GET_PARAM_TYPE(&pInput[i]), &dataLen));
    }
    varDataSetLen(output, dataLen);
    SCL_ERR_JRET(colDataSetVal(pOutputData, k, outputBuf, false));
  }

  pOutput->numOfRows = numOfRows;

_return:
  taosMemoryFreeClear(input);
  taosMemoryFreeClear(outputBuf);
  taosMemoryFreeClear(pInputData);

  SCL_RET(code);
}

int32_t concatWsFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  int32_t           code = TSDB_CODE_SUCCESS;
  SColumnInfoData **pInputData = taosMemoryCalloc(inputNum, sizeof(SColumnInfoData *));
  SColumnInfoData  *pOutputData = pOutput->columnData;
  char             *outputBuf = NULL;

  if (NULL == pInputData) {
    SCL_ERR_JRET(terrno);
  }

  int32_t inputLen = 0;
  int32_t numOfRows = 0;
  bool    hasNchar = (GET_PARAM_TYPE(pOutput) == TSDB_DATA_TYPE_NCHAR) ? true : false;
  for (int32_t i = 0; i < inputNum; ++i) {
    numOfRows = TMAX(pInput[i].numOfRows, numOfRows);
  }
  int32_t outputLen = VARSTR_HEADER_SIZE;
  for (int32_t i = 0; i < inputNum; ++i) {
    if (IS_NULL_TYPE(GET_PARAM_TYPE(&pInput[i]))) {
      colDataSetNNULL(pOutputData, 0, numOfRows);
      pOutput->numOfRows = numOfRows;
      goto _return;
    }
    pInputData[i] = pInput[i].columnData;
    int32_t factor = 1;
    if (hasNchar && (GET_PARAM_TYPE(&pInput[i]) == TSDB_DATA_TYPE_VARCHAR)) {
      factor = TSDB_NCHAR_SIZE;
    }

    if (i == 0) {
      // calculate required separator space
      outputLen += pInputData[i]->info.bytes * factor * (inputNum - 2);
    } else {
      outputLen += pInputData[i]->info.bytes * factor;
    }
  }

  outputBuf = taosMemoryCalloc(outputLen, 1);
  if (NULL == outputBuf) {
    SCL_ERR_JRET(terrno);
  }

  for (int32_t k = 0; k < numOfRows; ++k) {
    if (colDataIsNull_s(pInputData[0], k) || IS_NULL_TYPE(GET_PARAM_TYPE(&pInput[0]))) {
      colDataSetNULL(pOutputData, k);
      continue;
    }

    VarDataLenT dataLen = 0;
    bool        hasNull = false;
    char       *output = outputBuf;
    for (int32_t i = 1; i < inputNum; ++i) {
      if (colDataIsNull_s(pInputData[i], k) || IS_NULL_TYPE(GET_PARAM_TYPE(&pInput[i]))) {
        hasNull = true;
        break;
      }

      int32_t rowIdx = (pInput[i].numOfRows == 1) ? 0 : k;

      SCL_ERR_JRET(concatCopyHelper(colDataGetData(pInputData[i], rowIdx), output, hasNchar, GET_PARAM_TYPE(&pInput[i]), &dataLen));

      if (i < inputNum - 1) {
        // insert the separator
        char *sep = (pInput[0].numOfRows == 1) ? colDataGetData(pInputData[0], 0) : colDataGetData(pInputData[0], k);
        SCL_ERR_JRET(concatCopyHelper(sep, output, hasNchar, GET_PARAM_TYPE(&pInput[0]), &dataLen));
      }
    }

    if (hasNull) {
      colDataSetNULL(pOutputData, k);
      (void)memset(output, 0, dataLen);
    } else {
      varDataSetLen(output, dataLen);
      SCL_ERR_JRET(colDataSetVal(pOutputData, k, output, false));
    }
  }

  pOutput->numOfRows = numOfRows;

_return:
  taosMemoryFreeClear(outputBuf);
  taosMemoryFreeClear(pInputData);

  return code;
}

static int32_t doCaseConvFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput, _conv_fn convFn) {
  int32_t type = GET_PARAM_TYPE(pInput);

  SColumnInfoData *pInputData = pInput->columnData;
  SColumnInfoData *pOutputData = pOutput->columnData;

  int32_t outputLen = pInputData->varmeta.length;
  char   *outputBuf = taosMemoryCalloc(outputLen, 1);
  if (outputBuf == NULL) {
    SCL_ERR_RET(terrno);
  }

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
    int32_t code = colDataSetVal(pOutputData, i, output, false);
    if (TSDB_CODE_SUCCESS != code) {
      taosMemoryFreeClear(outputBuf);
      SCL_ERR_RET(code);
    }
    output += varDataTLen(output);
  }

  pOutput->numOfRows = pInput->numOfRows;
  taosMemoryFreeClear(outputBuf);

  return TSDB_CODE_SUCCESS;
}

static int32_t doTrimFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput, _trim_space_fn trimSpaceFn, _trim_fn trimFn) {
  int32_t          code = TSDB_CODE_SUCCESS;
  SColumnInfoData *pInputData[2];
  SColumnInfoData *pOutputData = pOutput[0].columnData;
  int32_t          outputLen;
  int32_t          numOfRows;
  pInputData[0] = pInput[0].columnData;
  if (inputNum == 3) {
    pInputData[1] = pInput[1].columnData;
    outputLen = pInputData[1]->info.bytes;
    numOfRows = TMAX(pInput[0].numOfRows, pInput[1].numOfRows);
  } else {
    outputLen = pInputData[0]->info.bytes;
    numOfRows = pInput[0].numOfRows;
  }
  char *outputBuf = taosMemoryCalloc(outputLen, 1);
  if (outputBuf == NULL) {
    SCL_ERR_RET(terrno);
  }

  char   *output = outputBuf;

  if (inputNum == 3) {
    bool hasNullType = (IS_NULL_TYPE(GET_PARAM_TYPE(&pInput[0])) || IS_NULL_TYPE(GET_PARAM_TYPE(&pInput[1])));

    if (hasNullType ||
        (pInput[0].numOfRows == 1 && colDataIsNull_s(pInputData[0], 0)) ||
        (pInput[1].numOfRows == 1 && colDataIsNull_s(pInputData[1], 0))) {
      colDataSetNNULL(pOutputData, 0, numOfRows);
    }

    for (int32_t i = 0; i < numOfRows; ++i) {
      int32_t colIdx1 = (pInput[0].numOfRows == 1) ? 0 : i;
      int32_t colIdx2 = (pInput[1].numOfRows == 1) ? 0 : i;
      if (colDataIsNull_s(pInputData[0], colIdx1) || colDataIsNull_s(pInputData[1], colIdx2)) {
        colDataSetNULL(pOutputData, i);
        continue;
      }
      SCL_ERR_JRET(trimFn(colDataGetData(pInputData[1], colIdx2), colDataGetData(pInputData[0], colIdx1),
                          output, GET_PARAM_TYPE(&pInput[1]), GET_PARAM_TYPE(&pInput[0])));
      SCL_ERR_JRET(colDataSetVal(pOutputData, i, output, false));
    }
  } else {
    for (int32_t i = 0; i < numOfRows; ++i) {
      if (colDataIsNull_s(pInputData[0], i)) {
        colDataSetNULL(pOutputData, i);
        continue;
      }
      int32_t type = GET_PARAM_TYPE(pInput);
      char   *input = colDataGetData(pInputData[0], i);
      int32_t len = varDataLen(input);
      int32_t charLen = (type == TSDB_DATA_TYPE_VARCHAR) ? len : len / TSDB_NCHAR_SIZE;
      trimSpaceFn(input, output, type, charLen);
      SCL_ERR_JRET(colDataSetVal(pOutputData, i, output, false));
    }
  }
  pOutput->numOfRows = numOfRows;
_return:
  taosMemoryFreeClear(outputBuf);
  return code;
}

static int32_t findPosBytes(char *orgStr, char *delimStr, int32_t orgLen, int32_t delimLen, int32_t charNums, bool isNchar) {
  int32_t charCount = 0;
  if (charNums > 0) {
    for (int32_t pos = 0; pos < orgLen; pos++) {
      if (delimStr) {
        if (pos + delimLen > orgLen)  {
          return orgLen;
        }
        if ((isCharStart(orgStr[pos]) || isNchar) && memcmp(orgStr + pos, delimStr, delimLen) == 0) {
          charCount++;
          if (charCount == charNums) {
            return pos;
          }
          pos = pos + delimLen - 1;
        }
      } else {
        if ((isCharStart(orgStr[pos]) || isNchar)) {
          charCount++;
          if (charCount == charNums) {
            return pos;
          }
        }
      }
    }
    return orgLen;
  } else {
    if (delimStr) {
      for (int32_t pos = orgLen - delimLen; pos >= 0; pos--) {
        if ((isCharStart(orgStr[pos]) || isNchar) && memcmp(orgStr + pos, delimStr, delimLen) == 0) {
          charCount++;
          if (charCount == -charNums) {
            return pos + delimLen;
          }
          pos = pos - delimLen + 1;
        }
      }
    } else {
      for (int32_t pos = orgLen - 1; pos >= 0; pos--) {
        if ((isCharStart(orgStr[pos]) || isNchar)) {
          charCount++;
          if (charCount == -charNums) {
            return pos;
          }
        }
      }
    }
    return 0;
  }
}

int32_t substrFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  int32_t code = TSDB_CODE_SUCCESS;
  SColumnInfoData *pInputData[3];
  SColumnInfoData *pOutputData = pOutput->columnData;

  for (int32_t i = 0; i < inputNum; ++i) {
    pInputData[i] = pInput[i].columnData;
  }

  int32_t outputLen = pInputData[0]->info.bytes;
  char *outputBuf = taosMemoryMalloc(outputLen);
  if (outputBuf == NULL) {
    qError("substr function memory allocation failure. size: %d", outputLen);
    return terrno;
  }

  int32_t numOfRows = 0;
  for (int32_t i = 0; i < inputNum; ++i) {
    numOfRows = TMAX(pInput[i].numOfRows, numOfRows);
  }

  bool hasNullType = (IS_NULL_TYPE(GET_PARAM_TYPE(&pInput[0])) || IS_NULL_TYPE(GET_PARAM_TYPE(&pInput[1])) ||
                      (inputNum == 3 && IS_NULL_TYPE(GET_PARAM_TYPE(&pInput[2]))));

  if (hasNullType ||
      (pInput[0].numOfRows == 1 && colDataIsNull_s(pInputData[0], 0)) ||
      (pInput[1].numOfRows == 1 && colDataIsNull_s(pInputData[1], 0)) ||
      (inputNum == 3 && (pInput[2].numOfRows == 1 && colDataIsNull_s(pInputData[2], 0)))) {
    colDataSetNNULL(pOutputData, 0, numOfRows);
    pOutput->numOfRows = numOfRows;
    goto _return;
  }

  int32_t colIdx[3];
  for (int32_t i = 0; i < numOfRows; ++i) {
    colIdx[0] = (pInput[0].numOfRows == 1) ? 0 : i;
    colIdx[1] = (pInput[1].numOfRows == 1) ? 0 : i;
    if (inputNum == 3) {
      colIdx[2] = (pInput[2].numOfRows == 1) ? 0 : i;
    }

    if (colDataIsNull_s(pInputData[0], colIdx[0]) || colDataIsNull_s(pInputData[1], colIdx[1]) ||
        (inputNum == 3 && colDataIsNull_s(pInputData[2], colIdx[2]))) {
      colDataSetNULL(pOutputData, i);
      continue;
    }

    int32_t subPos = 0;
    int32_t subLen = INT16_MAX;
    GET_TYPED_DATA(subPos, int32_t, GET_PARAM_TYPE(&pInput[1]), colDataGetData(pInputData[1], colIdx[1]));
    if (inputNum == 3) {
      GET_TYPED_DATA(subLen, int32_t, GET_PARAM_TYPE(&pInput[2]), colDataGetData(pInputData[2], colIdx[2]));
    }

    if (subPos == 0 || subLen < 1) {
      varDataSetLen(outputBuf, 0);
      SCL_ERR_JRET(colDataSetVal(pOutputData, i, outputBuf, false));
      continue;
    }

    char   *input = colDataGetData(pInputData[0], colIdx[0]);
    int32_t len = varDataLen(input);
    int32_t startPosBytes;
    int32_t endPosBytes = len;
    if (subPos > 0) {
      startPosBytes = (GET_PARAM_TYPE(&pInput[0]) == TSDB_DATA_TYPE_VARCHAR) ? findPosBytes(varDataVal(input), NULL, varDataLen(input), -1, subPos, false) : (subPos - 1) * TSDB_NCHAR_SIZE;
      startPosBytes = TMIN(startPosBytes, len);
    } else {
      startPosBytes =
          (GET_PARAM_TYPE(&pInput[0]) == TSDB_DATA_TYPE_VARCHAR) ? findPosBytes(varDataVal(input), NULL, varDataLen(input), -1, subPos, false) : len + subPos * TSDB_NCHAR_SIZE;
      startPosBytes = TMAX(startPosBytes, 0);
    }
    if (inputNum == 3) {
      endPosBytes =
          (GET_PARAM_TYPE(&pInput[0]) == TSDB_DATA_TYPE_VARCHAR)
              ? startPosBytes + findPosBytes(varDataVal(input) + startPosBytes, NULL, varDataLen(input) - startPosBytes, -1, subLen + 1, false)
              : startPosBytes + subLen * TSDB_NCHAR_SIZE;
      endPosBytes = TMIN(endPosBytes, len);
    }

    char   *output = outputBuf;
    int32_t resLen = endPosBytes - startPosBytes;
    if (resLen > 0) {
      (void)memcpy(varDataVal(output), varDataVal(input) + startPosBytes, resLen);
      varDataSetLen(output, resLen);
    } else {
      varDataSetLen(output, 0);
    }

    SCL_ERR_JRET(colDataSetVal(pOutputData, i, outputBuf, false));
  }

  pOutput->numOfRows = numOfRows;
_return:
  taosMemoryFreeClear(outputBuf);

  return code;
}

int32_t md5Function(SScalarParam* pInput, int32_t inputNum, SScalarParam* pOutput) {
  SColumnInfoData *pInputData = pInput->columnData;
  SColumnInfoData *pOutputData = pOutput->columnData;
  int32_t bufLen = TMAX(MD5_OUTPUT_LEN + VARSTR_HEADER_SIZE + 1, pInputData->info.bytes);
  char* pOutputBuf = taosMemoryMalloc(bufLen);
  if (!pOutputBuf) {
    qError("md5 function alloc memory failed");
    return terrno;
  }
  for (int32_t i = 0; i < pInput->numOfRows; ++i) {
    if (colDataIsNull_s(pInputData, i)) {
      colDataSetNULL(pOutputData, i);
      continue;
    }
    char *input = colDataGetData(pInput[0].columnData, i);
    if (bufLen < varDataLen(input) + VARSTR_HEADER_SIZE) {
      bufLen = varDataLen(input) + VARSTR_HEADER_SIZE;
      pOutputBuf = taosMemoryRealloc(pOutputBuf, bufLen);
      if (!pOutputBuf) {
        qError("md5 function alloc memory failed");
        return terrno;
      }
    }
    char *output = pOutputBuf;
    (void)memcpy(varDataVal(output), varDataVal(input), varDataLen(input));
    int32_t len = taosCreateMD5Hash(varDataVal(output), varDataLen(input));
    varDataSetLen(output, len);
    int32_t code = colDataSetVal(pOutputData, i, output, false);
    if (TSDB_CODE_SUCCESS != code) {
      taosMemoryFreeClear(pOutputBuf);
      SCL_ERR_RET(code);
    }

  }
  pOutput->numOfRows = pInput->numOfRows;
  taosMemoryFreeClear(pOutputBuf);
  return TSDB_CODE_SUCCESS;
}

static void getAsciiChar(int32_t num, char **output) {
  if (num & 0xFF000000L) {
    INT4TOCHAR(*output, num);
  } else if (num & 0xFF0000L) {
    INT3TOCHAR(*output, num);
  } else if (num & 0xFF00L) {
    INT2TOCHAR(*output, num);
  } else {
    INT1TOCHAR(*output, num);
  }
}
int32_t charFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t outputLen = inputNum * 4 + 2;
  char   *outputBuf = taosMemoryCalloc(outputLen, 1);
  if (outputBuf == NULL) {
    SCL_ERR_RET(terrno);
  }
  int32_t numOfRows = 0;
  for (int32_t i = 0; i < inputNum; ++i) {
    numOfRows = TMAX(numOfRows, pInput[i].numOfRows);
  }
  for (int32_t i = 0; i < numOfRows; ++i) {
    char   *output = varDataVal(outputBuf);
    for (int32_t j = 0; j < inputNum; ++j) {
      int32_t colIdx = (pInput[j].numOfRows == 1) ? 0 : i;
      int32_t num;
      if (colDataIsNull_s(pInput[j].columnData, i)) {
        continue;
      } else if (IS_NUMERIC_TYPE(GET_PARAM_TYPE(&pInput[j]))) {
        GET_TYPED_DATA(num, int32_t, GET_PARAM_TYPE(&pInput[j]), colDataGetData(pInput[j].columnData, colIdx));
        getAsciiChar(num, &output);
      } else if (TSDB_DATA_TYPE_BINARY == GET_PARAM_TYPE(&pInput[j])) {
        num = taosStr2Int32(varDataVal(colDataGetData(pInput[j].columnData, colIdx)), NULL, 10);
        getAsciiChar(num, &output);
      } else if (TSDB_DATA_TYPE_NCHAR == GET_PARAM_TYPE(&pInput[j])) {
        char   *convBuf = taosMemoryMalloc(GET_PARAM_BYTES(&pInput[j]));
        if (convBuf == NULL) {
          SCL_ERR_RET(terrno);
        }
        int32_t len = taosUcs4ToMbs((TdUcs4 *)varDataVal(colDataGetData(pInput[j].columnData, colIdx)), varDataLen(colDataGetData(pInput[j].columnData, colIdx)), convBuf);
        if (len < 0) {
          taosMemoryFreeClear(convBuf);
          code = TSDB_CODE_SCALAR_CONVERT_ERROR;
          goto _return;
        }
        convBuf[len] = 0;
        num = taosStr2Int32(convBuf, NULL, 10);
        getAsciiChar(num, &output);
        taosMemoryFreeClear(convBuf);
      } else {
        code = TSDB_CODE_FUNC_FUNTION_PARA_TYPE;
        goto _return;
      }
    }
    varDataSetLen(outputBuf, output - varDataVal(outputBuf));
    SCL_ERR_JRET(colDataSetVal(pOutput->columnData, i, outputBuf, false));
  }
  pOutput->numOfRows = pInput->numOfRows;

_return:
  taosMemoryFreeClear(outputBuf);
  return code;
}

int32_t asciiFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  int32_t type = GET_PARAM_TYPE(pInput);

  SColumnInfoData *pInputData = pInput->columnData;
  SColumnInfoData *pOutputData = pOutput->columnData;

  uint8_t *out = (uint8_t *)pOutputData->pData;

  for (int32_t i = 0; i < pInput->numOfRows; ++i) {
    if (colDataIsNull_s(pInputData, i)) {
      colDataSetNULL(pOutputData, i);
      continue;
    }
    if (type == TSDB_DATA_TYPE_NCHAR) {
      char   *in = varDataVal(colDataGetData(pInputData, i));
      int32_t inLen = varDataLen(colDataGetData(pInputData, i));
      SCL_ERR_RET(convBetweenNcharAndVarchar(varDataVal(colDataGetData(pInputData, i)), &in,
                                             varDataLen(colDataGetData(pInputData, i)), &inLen,
                                             TSDB_DATA_TYPE_VARBINARY));
      out[i] = (uint8_t)(in)[0];
      taosMemoryFreeClear(in);
    } else {
      char *in = colDataGetData(pInputData, i);
      out[i] = (uint8_t)(varDataVal(in))[0];
    }
  }

  pOutput->numOfRows = pInput->numOfRows;
  return TSDB_CODE_SUCCESS;
}

static int32_t findPosChars(char *orgStr, char *delimStr, int32_t orgLen, int32_t delimLen, bool isNchar) {
  int32_t charCount = 0;
  for (int32_t pos = 0; pos < orgLen; pos += isNchar ? TSDB_NCHAR_SIZE : 1) {
    if (isNchar || isCharStart(orgStr[pos])) {
      if (pos + delimLen > orgLen) {
        return 0;
      }
      if (memcmp(orgStr + pos, delimStr, delimLen) == 0) {
        return charCount + 1;
      } else {
        charCount++;
      }
    }
  }
  return 0;
}

int32_t positionFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  int32_t          code = TSDB_CODE_SUCCESS;
  int32_t          numOfRows = 0;
  SColumnInfoData *pInputData[2];
  SColumnInfoData *pOutputData = pOutput[0].columnData;

  pInputData[0] = pInput[0].columnData;
  pInputData[1] = pInput[1].columnData;
  numOfRows = TMAX(pInput[0].numOfRows, pInput[1].numOfRows);

  bool hasNullType = (IS_NULL_TYPE(GET_PARAM_TYPE(&pInput[0])) || IS_NULL_TYPE(GET_PARAM_TYPE(&pInput[1])));

  if (hasNullType ||
      (pInput[0].numOfRows == 1 && colDataIsNull_s(pInputData[0], 0)) ||
      (pInput[1].numOfRows == 1 && colDataIsNull_s(pInputData[1], 0))) {
    colDataSetNNULL(pOutputData, 0, numOfRows);
  }

  for (int32_t i = 0; i < numOfRows; ++i) {
    int32_t colIdx1 = (pInput[0].numOfRows == 1) ? 0 : i;
    int32_t colIdx2 = (pInput[1].numOfRows == 1) ? 0 : i;
    if (colDataIsNull_s(pInputData[0], colIdx1) || colDataIsNull_s(pInputData[1], colIdx2)) {
      colDataSetNULL(pOutputData, i);
      continue;
    }
    int64_t offset = 0;
    if (varDataLen(colDataGetData(pInputData[0], colIdx1)) == 0) {
      offset = 1;
      colDataSetInt64(pOutputData, i, &offset);
      continue;
    }

    char   *substr = varDataVal(colDataGetData(pInputData[0], colIdx1));
    char   *orgstr = varDataVal(colDataGetData(pInputData[1], colIdx2));
    int32_t subLen = varDataLen(colDataGetData(pInputData[0], colIdx1));
    int32_t orgLen = varDataLen(colDataGetData(pInputData[1], colIdx2));
    bool    needFreeSub = false;
    if (GET_PARAM_TYPE(&pInput[1]) != GET_PARAM_TYPE(&pInput[0])) {
      SCL_ERR_RET(convBetweenNcharAndVarchar(varDataVal(colDataGetData(pInputData[0], colIdx1)), &substr, varDataLen(colDataGetData(pInputData[0], colIdx1)), &subLen, GET_PARAM_TYPE(&pInput[1])));
      needFreeSub = true;
    }

    offset = findPosChars(orgstr, substr, orgLen, subLen, GET_PARAM_TYPE(&pInput[1]) == TSDB_DATA_TYPE_NCHAR);
    if (needFreeSub) {
      taosMemoryFreeClear(substr);
    }
    colDataSetInt64(pOutput->columnData, i, &offset);
  }

  pOutput->numOfRows = numOfRows;
  return code;
}

int32_t trimFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  // trim space
  uint8_t trimType = 0;
  GET_TYPED_DATA(trimType, int32_t, GET_PARAM_TYPE(&pInput[inputNum - 1]), pInput[inputNum - 1].columnData->pData);
  switch (trimType) {
    case TRIM_TYPE_LEADING: {
      SCL_ERR_RET(doTrimFunction(pInput, inputNum, pOutput, tltrimspace, tltrim));
      break;
    }
    case TRIM_TYPE_TRAILING: {
      SCL_ERR_RET(doTrimFunction(pInput, inputNum, pOutput, trtrimspace, trtrim));
      break;
    }
    case TRIM_TYPE_BOTH: {
      SCL_ERR_RET(doTrimFunction(pInput, inputNum, pOutput, tlrtrimspace, tlrtrim));
      break;
    }
  }
  return TSDB_CODE_SUCCESS;
}

int32_t replaceFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  int32_t          code = TSDB_CODE_SUCCESS;
  SColumnInfoData *pInputData[3];
  SColumnInfoData *pOutputData = pOutput[0].columnData;
  int32_t          outputLen;
  int32_t          numOfRows = 0;

  pInputData[0] = pInput[0].columnData;
  pInputData[1] = pInput[1].columnData;
  pInputData[2] = pInput[2].columnData;

  for (int i = 0; i < 3; i++) {
    numOfRows = TMAX(numOfRows, pInput[i].numOfRows);
  }

  int8_t  orgType = pInputData[0]->info.type;
  int8_t  toType = pInputData[2]->info.type;
  int32_t orgLength = pInputData[0]->info.bytes - VARSTR_HEADER_SIZE;
  int32_t toLength = pInputData[2]->info.bytes - VARSTR_HEADER_SIZE;

  if (orgType == TSDB_DATA_TYPE_NCHAR && toType != orgType) {
    toLength = toLength * TSDB_NCHAR_SIZE;
  }
  outputLen = toLength == 0 ? orgLength : TMIN(TSDB_MAX_FIELD_LEN, orgLength * toLength);

  if (GET_PARAM_TYPE(&pInput[0]) == TSDB_DATA_TYPE_NULL ||
      GET_PARAM_TYPE(&pInput[1]) == TSDB_DATA_TYPE_NULL ||
      GET_PARAM_TYPE(&pInput[2]) == TSDB_DATA_TYPE_NULL ||
      (pInput[0].numOfRows == 1 && colDataIsNull_s(pInputData[0], 0)) ||
      (pInput[1].numOfRows == 1 && colDataIsNull_s(pInputData[1], 0)) ||
      (pInput[2].numOfRows == 1 && colDataIsNull_s(pInputData[2], 0))) {
    colDataSetNNULL(pOutputData, 0, numOfRows);
    pOutput->numOfRows = numOfRows;
    return TSDB_CODE_SUCCESS;
  }

  char *outputBuf = taosMemoryCalloc(outputLen + VARSTR_HEADER_SIZE, 1);
  if (NULL == outputBuf) {
    SCL_ERR_RET(terrno);
  }

  for (int32_t i = 0; i < numOfRows; ++i) {
    int32_t colIdx1 = (pInput[0].numOfRows == 1) ? 0 : i;
    int32_t colIdx2 = (pInput[1].numOfRows == 1) ? 0 : i;
    int32_t colIdx3 = (pInput[2].numOfRows == 1) ? 0 : i;
    if (colDataIsNull_s(pInputData[0], colIdx1) || colDataIsNull_s(pInputData[1], colIdx2) || colDataIsNull_s(pInputData[2], colIdx3)) {
      colDataSetNULL(pOutputData, i);
      continue;
    }
    char   *output = outputBuf + VARSTR_HEADER_SIZE;
    int32_t totalLen = 0;

    char   *orgStr = varDataVal(colDataGetData(pInputData[0], colIdx1));
    int32_t orgLen = varDataLen(colDataGetData(pInputData[0], colIdx1));
    char   *fromStr = varDataVal(colDataGetData(pInputData[1], colIdx2));
    int32_t fromLen = varDataLen(colDataGetData(pInputData[1], colIdx2));
    char   *toStr = varDataVal(colDataGetData(pInputData[2], colIdx3));
    int32_t toLen = varDataLen(colDataGetData(pInputData[2], colIdx3));
    bool    needFreeFrom = false;
    bool    needFreeTo = false;

    if (fromLen == 0 || orgLen == 0) {
      (void)memcpy(output, orgStr, orgLen);
      totalLen = orgLen;
      varDataSetLen(outputBuf, totalLen);
      SCL_ERR_JRET(colDataSetVal(pOutputData, i, outputBuf, false));
      continue;
    }

    if (GET_PARAM_TYPE(&pInput[1]) != GET_PARAM_TYPE(&pInput[0])) {
      SCL_ERR_JRET(convBetweenNcharAndVarchar(varDataVal(colDataGetData(pInputData[1], colIdx2)), &fromStr,
                                             varDataLen(colDataGetData(pInputData[1], colIdx2)), &fromLen,
                                             GET_PARAM_TYPE(&pInput[0])));
      needFreeFrom = true;
    }
    if (GET_PARAM_TYPE(&pInput[2]) != GET_PARAM_TYPE(&pInput[0])) {
      code = convBetweenNcharAndVarchar(varDataVal(colDataGetData(pInputData[2], colIdx3)), &toStr,
                                        varDataLen(colDataGetData(pInputData[2], colIdx3)), &toLen,
                                        GET_PARAM_TYPE(&pInput[0]));
      if (TSDB_CODE_SUCCESS != code) {
        if (needFreeFrom) {
          taosMemoryFreeClear(fromStr);
        }
        goto _return;
      }
      needFreeTo = true;
    }

    int32_t pos = 0;
    while (pos < orgLen) {
      if (orgLen - pos < fromLen) {
        (void)memcpy(output, orgStr + pos, orgLen - pos);
        output += orgLen - pos;
        totalLen += orgLen - pos;
        break;
      }
      if (memcmp(orgStr + pos, fromStr, fromLen) == 0 &&
          (pos + fromLen == orgLen ||
           isCharStart(orgStr[pos + fromLen]) ||
           GET_PARAM_TYPE(&pInput[0]) == TSDB_DATA_TYPE_NCHAR)) {
        (void)memcpy(output, toStr, toLen);
        output += toLen;
        pos += fromLen;
        totalLen += toLen;
      } else {
        int32_t charLen = GET_PARAM_TYPE(&pInput[0]) == TSDB_DATA_TYPE_NCHAR ? TSDB_NCHAR_SIZE : getCharLen(orgStr + pos);
        (void)memcpy(output, orgStr + pos, charLen);
        output += charLen;
        totalLen += charLen;
        pos += charLen;
      }
    }
    if (needFreeTo) {
      taosMemoryFreeClear(toStr);
    }
    if (needFreeFrom) {
      taosMemoryFreeClear(fromStr);
    }
    if (totalLen > TSDB_MAX_FIELD_LEN) {
      SCL_ERR_JRET(TSDB_CODE_FUNC_INVALID_RES_LENGTH);
    }
    varDataSetLen(outputBuf, totalLen);
    SCL_ERR_JRET(colDataSetVal(pOutputData, i, outputBuf, false));
  }
  pOutput->numOfRows = numOfRows;
_return:
  taosMemoryFreeClear(outputBuf);
  return code;
}

int32_t substrIdxFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  int32_t          code = TSDB_CODE_SUCCESS;
  SColumnInfoData *pInputData[3];
  SColumnInfoData *pOutputData = pOutput[0].columnData;
  int32_t          outputLen;
  int32_t          numOfRows = 0;

  pInputData[0] = pInput[0].columnData;
  pInputData[1] = pInput[1].columnData;
  pInputData[2] = pInput[2].columnData;

  for (int32_t i = 0; i < inputNum; ++i) {
    numOfRows = TMAX(numOfRows, pInput[i].numOfRows);
  }

  outputLen = pInputData[0]->info.bytes;
  if (GET_PARAM_TYPE(&pInput[0]) == TSDB_DATA_TYPE_NULL || GET_PARAM_TYPE(&pInput[1]) == TSDB_DATA_TYPE_NULL || GET_PARAM_TYPE(&pInput[2]) == TSDB_DATA_TYPE_NULL) {
    colDataSetNNULL(pOutputData, 0, numOfRows);
    pOutput->numOfRows = numOfRows;
    return TSDB_CODE_SUCCESS;
  }
  char *outputBuf = taosMemoryCalloc(outputLen + VARSTR_HEADER_SIZE, 1);
  if (NULL == outputBuf) {
    SCL_ERR_RET(terrno);
  }

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

    int32_t colIdx1 = (pInput[0].numOfRows == 1) ? 0 : k;
    int32_t colIdx2 = (pInput[1].numOfRows == 1) ? 0 : k;
    int32_t count = 0;
    char   *orgStr = varDataVal(colDataGetData(pInputData[0], colIdx1));
    int32_t orgLen = varDataLen(colDataGetData(pInputData[0], colIdx1));
    char   *delimStr = varDataVal(colDataGetData(pInputData[1], colIdx2));
    int32_t delimLen = varDataLen(colDataGetData(pInputData[1], colIdx2));
    bool    needFreeDelim = false;
    GET_TYPED_DATA(count, int32_t, GET_PARAM_TYPE(&pInput[2]), colDataGetData(pInputData[2], (pInput[2].numOfRows == 1) ? 0 : k));

    int32_t startPosBytes;
    int32_t endPosBytes;
    if (GET_PARAM_TYPE(&pInput[0]) != GET_PARAM_TYPE(&pInput[1])) {
      SCL_ERR_JRET(convBetweenNcharAndVarchar(varDataVal(colDataGetData(pInputData[1], colIdx2)), &delimStr,
                                              varDataLen(colDataGetData(pInputData[1], colIdx2)), &delimLen,
                                              GET_PARAM_TYPE(&pInput[0])));
      needFreeDelim = true;
    }

    if (count > 0) {
      startPosBytes = 0;
      endPosBytes = findPosBytes(orgStr, delimStr, orgLen, delimLen, count, GET_PARAM_TYPE(&pInput[0]) == TSDB_DATA_TYPE_NCHAR);
    } else if (count < 0) {
      startPosBytes = findPosBytes(orgStr, delimStr, orgLen, delimLen, count, GET_PARAM_TYPE(&pInput[0]) == TSDB_DATA_TYPE_NCHAR);
      endPosBytes = orgLen;
    } else {
      startPosBytes = endPosBytes = 0;
    }

    char   *output = outputBuf;
    int32_t resLen = endPosBytes - startPosBytes;
    if (resLen > 0) {
      (void)memcpy(varDataVal(output), orgStr + startPosBytes, resLen);
      varDataSetLen(output, resLen);
    } else {
      varDataSetLen(output, 0);
    }
    if (needFreeDelim) {
      taosMemoryFreeClear(delimStr);
    }

    SCL_ERR_JRET(colDataSetVal(pOutputData, k, output, false));
  }
  pOutput->numOfRows = numOfRows;
_return:
  taosMemoryFreeClear(outputBuf);
  return code;
}

static int32_t repeatStringHelper(char *input, int32_t inputLen, int32_t count, char *output) {
  for (int32_t i = 0; i < count; ++i) {
    (void)memcpy(output, input, inputLen);
    output += inputLen;
  }
  return TSDB_CODE_SUCCESS;
}

int32_t repeatFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  int32_t          code = TSDB_CODE_SUCCESS;
  SColumnInfoData *pInputData[2];
  SColumnInfoData *pOutputData = pOutput[0].columnData;
  int32_t          outputLen;
  int32_t          numOfRows;
  int32_t          maxCount = 0;

  for (int32_t i = 0; i < pInput[1].numOfRows; i++) {
    int32_t tmpCount = 0;
    if (colDataIsNull_s(pInput[1].columnData, i)) {
      continue;
    }
    GET_TYPED_DATA(tmpCount, int32_t, GET_PARAM_TYPE(&pInput[1]), colDataGetData(pInput[1].columnData, i));
    maxCount = TMAX(maxCount, tmpCount);
  }
  pInputData[0] = pInput[0].columnData;
  pInputData[1] = pInput[1].columnData;
  outputLen = (int32_t)(pInputData[0]->info.bytes * maxCount + VARSTR_HEADER_SIZE);
  if (outputLen > TSDB_MAX_FIELD_LEN) {
    return TSDB_CODE_FUNC_INVALID_RES_LENGTH;
  }
  numOfRows = TMAX(pInput[0].numOfRows, pInput[1].numOfRows);
  char *outputBuf = taosMemoryCalloc(outputLen, 1);
  if (outputBuf == NULL) {
    SCL_ERR_RET(terrno);
  }

  char   *output = outputBuf;

  bool hasNullType = (IS_NULL_TYPE(GET_PARAM_TYPE(&pInput[0])) || IS_NULL_TYPE(GET_PARAM_TYPE(&pInput[1])));
  if (pInput[0].numOfRows == pInput[1].numOfRows) {
    for (int32_t i = 0; i < numOfRows; ++i) {
      output = outputBuf;
      if (colDataIsNull_s(pInputData[0], i) || colDataIsNull_s(pInputData[1], i) || hasNullType) {
        colDataSetNULL(pOutputData, i);
        continue;
      }
      int32_t count = 0;
      GET_TYPED_DATA(count, int32_t, GET_PARAM_TYPE(&pInput[1]), colDataGetData(pInput[1].columnData, i));
      if (count <= 0) {
        varDataSetLen(output, 0);
        SCL_ERR_JRET(colDataSetVal(pOutputData, i, outputBuf, false));
      } else {
        char   *orgStr = colDataGetData(pInputData[0], i);
        varDataSetLen(output, varDataLen(orgStr) * count);
        SCL_ERR_JRET(repeatStringHelper(varDataVal(orgStr), varDataLen(orgStr), count, varDataVal(output)));
        SCL_ERR_JRET(colDataSetVal(pOutputData, i, outputBuf, false));
      }
    }
  } else if (pInput[0].numOfRows == 1) {
    if (colDataIsNull_s(pInputData[0], 0) || hasNullType) {
      colDataSetNNULL(pOutputData, 0, pInput[1].numOfRows);
    } else {
      for (int32_t i = 0; i < numOfRows; ++i) {
        output = outputBuf;
        if (colDataIsNull_s(pInputData[1], i)) {
          colDataSetNULL(pOutputData, i);
          continue;
        }
        int32_t count = 0;
        GET_TYPED_DATA(count, int32_t, GET_PARAM_TYPE(&pInput[1]), colDataGetData(pInput[1].columnData, i));
        if (count <= 0) {
          varDataSetLen(output, 0);
          SCL_ERR_JRET(colDataSetVal(pOutputData, i, outputBuf, false));
        } else {
          char   *orgStr = colDataGetData(pInputData[0], 0);
          varDataSetLen(output, varDataLen(orgStr) * count);
          SCL_ERR_JRET(repeatStringHelper(varDataVal(orgStr), varDataLen(orgStr), count, varDataVal(output)));
          SCL_ERR_JRET(colDataSetVal(pOutputData, i, outputBuf, false));
        }
      }
    }
  } else if (pInput[1].numOfRows == 1) {
    if (colDataIsNull_s(pInputData[1], 0) || hasNullType) {
      colDataSetNNULL(pOutputData, 0, pInput[0].numOfRows);
    } else {
      for (int32_t i = 0; i < numOfRows; ++i) {
        output = outputBuf;
        if (colDataIsNull_s(pInputData[0], i)) {
          colDataSetNULL(pOutputData, i);
          continue;
        }
        int32_t count = 0;
        GET_TYPED_DATA(count, int32_t, GET_PARAM_TYPE(&pInput[1]), colDataGetData(pInput[1].columnData, 0));
        if (count <= 0) {
          varDataSetLen(output, 0);
          SCL_ERR_JRET(colDataSetVal(pOutputData, i, outputBuf, false));
        } else {
          char   *orgStr = colDataGetData(pInputData[0], i);
          varDataSetLen(output, varDataLen(orgStr) * count);
          SCL_ERR_JRET(repeatStringHelper(varDataVal(orgStr), varDataLen(orgStr), count, varDataVal(output)));
          SCL_ERR_JRET(colDataSetVal(pOutputData, i, outputBuf, false));
        }
      }
    }
  }

  pOutput->numOfRows = numOfRows;
_return:
  taosMemoryFreeClear(outputBuf);
  return code;
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
  int32_t bufSize = TSDB_MAX_FIELD_LEN + 1;
  char   *buf = taosMemoryMalloc(bufSize);

  if (convBuf == NULL || output == NULL || buf == NULL) {
    code = terrno;
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
          (void)memcpy(buf, varDataVal(input), varDataLen(input));
          buf[varDataLen(input)] = 0;
          *(int8_t *)output = taosStr2Int8(buf, NULL, 10);
        } else if (inputType == TSDB_DATA_TYPE_NCHAR) {
          int32_t len = taosUcs4ToMbs((TdUcs4 *)varDataVal(input), varDataLen(input), convBuf);
          if (len < 0) {
            code = TSDB_CODE_SCALAR_CONVERT_ERROR;
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
          (void)memcpy(buf, varDataVal(input), varDataLen(input));
          buf[varDataLen(input)] = 0;
          *(int16_t *)output = taosStr2Int16(buf, NULL, 10);
        } else if (inputType == TSDB_DATA_TYPE_NCHAR) {
          int32_t len = taosUcs4ToMbs((TdUcs4 *)varDataVal(input), varDataLen(input), convBuf);
          if (len < 0) {
            code = TSDB_CODE_SCALAR_CONVERT_ERROR;
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
          (void)memcpy(buf, varDataVal(input), varDataLen(input));
          buf[varDataLen(input)] = 0;
          *(int32_t *)output = taosStr2Int32(buf, NULL, 10);
        } else if (inputType == TSDB_DATA_TYPE_NCHAR) {
          int32_t len = taosUcs4ToMbs((TdUcs4 *)varDataVal(input), varDataLen(input), convBuf);
          if (len < 0) {
            code = TSDB_CODE_SCALAR_CONVERT_ERROR;
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
          (void)memcpy(buf, varDataVal(input), varDataLen(input));
          buf[varDataLen(input)] = 0;
          *(int64_t *)output = taosStr2Int64(buf, NULL, 10);
        } else if (inputType == TSDB_DATA_TYPE_NCHAR) {
          int32_t len = taosUcs4ToMbs((TdUcs4 *)varDataVal(input), varDataLen(input), convBuf);
          if (len < 0) {
            code = TSDB_CODE_SCALAR_CONVERT_ERROR;
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
          (void)memcpy(buf, varDataVal(input), varDataLen(input));
          buf[varDataLen(input)] = 0;
          *(uint8_t *)output = taosStr2UInt8(buf, NULL, 10);
        } else if (inputType == TSDB_DATA_TYPE_NCHAR) {
          int32_t len = taosUcs4ToMbs((TdUcs4 *)varDataVal(input), varDataLen(input), convBuf);
          if (len < 0) {
            code = TSDB_CODE_SCALAR_CONVERT_ERROR;
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
          (void)memcpy(buf, varDataVal(input), varDataLen(input));
          buf[varDataLen(input)] = 0;
          *(uint16_t *)output = taosStr2UInt16(buf, NULL, 10);
        } else if (inputType == TSDB_DATA_TYPE_NCHAR) {
          int32_t len = taosUcs4ToMbs((TdUcs4 *)varDataVal(input), varDataLen(input), convBuf);
          if (len < 0) {
            code = TSDB_CODE_SCALAR_CONVERT_ERROR;
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
          (void)memcpy(buf, varDataVal(input), varDataLen(input));
          buf[varDataLen(input)] = 0;
          *(uint32_t *)output = taosStr2UInt32(buf, NULL, 10);
        } else if (inputType == TSDB_DATA_TYPE_NCHAR) {
          int32_t len = taosUcs4ToMbs((TdUcs4 *)varDataVal(input), varDataLen(input), convBuf);
          if (len < 0) {
            code = TSDB_CODE_SCALAR_CONVERT_ERROR;
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
          (void)memcpy(buf, varDataVal(input), varDataLen(input));
          buf[varDataLen(input)] = 0;
          *(uint64_t *)output = taosStr2UInt64(buf, NULL, 10);
        } else if (inputType == TSDB_DATA_TYPE_NCHAR) {
          int32_t len = taosUcs4ToMbs((TdUcs4 *)varDataVal(input), varDataLen(input), convBuf);
          if (len < 0) {
            code = TSDB_CODE_SCALAR_CONVERT_ERROR;
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
          (void)memcpy(buf, varDataVal(input), varDataLen(input));
          buf[varDataLen(input)] = 0;
          *(float *)output = taosStr2Float(buf, NULL);
        } else if (inputType == TSDB_DATA_TYPE_NCHAR) {
          int32_t len = taosUcs4ToMbs((TdUcs4 *)varDataVal(input), varDataLen(input), convBuf);
          if (len < 0) {
            code = TSDB_CODE_SCALAR_CONVERT_ERROR;
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
          (void)memcpy(buf, varDataVal(input), varDataLen(input));
          buf[varDataLen(input)] = 0;
          *(double *)output = taosStr2Double(buf, NULL);
        } else if (inputType == TSDB_DATA_TYPE_NCHAR) {
          int32_t len = taosUcs4ToMbs((TdUcs4 *)varDataVal(input), varDataLen(input), convBuf);
          if (len < 0) {
            code = TSDB_CODE_SCALAR_CONVERT_ERROR;
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
          (void)memcpy(buf, varDataVal(input), varDataLen(input));
          buf[varDataLen(input)] = 0;
          *(bool *)output = taosStr2Int8(buf, NULL, 10);
        } else if (inputType == TSDB_DATA_TYPE_NCHAR) {
          int32_t len = taosUcs4ToMbs((TdUcs4 *)varDataVal(input), varDataLen(input), convBuf);
          if (len < 0) {
            code = TSDB_CODE_SCALAR_CONVERT_ERROR;
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
          // NOTE: snprintf will append '\0' at the end of string
          int32_t len = tsnprintf(varDataVal(output), outputLen + TSDB_NCHAR_SIZE - VARSTR_HEADER_SIZE, "%.*s",
                                 (int32_t)(outputLen - VARSTR_HEADER_SIZE), *(int8_t *)input ? "true" : "false");
          varDataSetLen(output, len);
        } else if (inputType == TSDB_DATA_TYPE_BINARY) {
          int32_t len = TMIN(varDataLen(input), outputLen - VARSTR_HEADER_SIZE);
          (void)memcpy(varDataVal(output), varDataVal(input), len);
          varDataSetLen(output, len);
        } else if (inputType == TSDB_DATA_TYPE_NCHAR) {
          int32_t len = taosUcs4ToMbs((TdUcs4 *)varDataVal(input), varDataLen(input), convBuf);
          if (len < 0) {
            code = TSDB_CODE_SCALAR_CONVERT_ERROR;
            goto _end;
          }
          len = TMIN(len, outputLen - VARSTR_HEADER_SIZE);
          (void)memcpy(varDataVal(output), convBuf, len);
          varDataSetLen(output, len);
        } else {
          int32_t outputSize = (outputLen - VARSTR_HEADER_SIZE) < bufSize ? (outputLen - VARSTR_HEADER_SIZE + 1): bufSize;
          NUM_TO_STRING(inputType, input, outputSize, buf);
          int32_t len = (int32_t)strlen(buf);
          len = (outputLen - VARSTR_HEADER_SIZE) > len ? len : (outputLen - VARSTR_HEADER_SIZE);
          (void)memcpy(varDataVal(output), buf, len);
          varDataSetLen(output, len);
        }
        break;
      }
      case TSDB_DATA_TYPE_VARBINARY:{
        if (inputType == TSDB_DATA_TYPE_BINARY) {
          int32_t len = TMIN(varDataLen(input), outputLen - VARSTR_HEADER_SIZE);
          (void)memcpy(varDataVal(output), varDataVal(input), len);
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
          len = tsnprintf(tmp, sizeof(tmp), "%.*s", outputCharLen, *(int8_t *)input ? "true" : "false");
          bool ret = taosMbsToUcs4(tmp, len, (TdUcs4 *)varDataVal(output), outputLen - VARSTR_HEADER_SIZE, &len);
          if (!ret) {
            code = TSDB_CODE_SCALAR_CONVERT_ERROR;
            goto _end;
          }

          varDataSetLen(output, len);
        } else if (inputType == TSDB_DATA_TYPE_BINARY) {
          len = outputCharLen > varDataLen(input) ? varDataLen(input) : outputCharLen;
          bool ret = taosMbsToUcs4(input + VARSTR_HEADER_SIZE, len, (TdUcs4 *)varDataVal(output),
                                   outputLen - VARSTR_HEADER_SIZE, &len);
          if (!ret) {
            code = TSDB_CODE_SCALAR_CONVERT_ERROR;
            goto _end;
          }
          varDataSetLen(output, len);
        } else if (inputType == TSDB_DATA_TYPE_NCHAR) {
          len = TMIN(outputLen - VARSTR_HEADER_SIZE, varDataLen(input));
          (void)memcpy(output, input, len + VARSTR_HEADER_SIZE);
          varDataSetLen(output, len);
        } else {
          NUM_TO_STRING(inputType, input, bufSize, buf);
          len = (int32_t)strlen(buf);
          len = outputCharLen > len ? len : outputCharLen;
          bool ret = taosMbsToUcs4(buf, len, (TdUcs4 *)varDataVal(output), outputLen - VARSTR_HEADER_SIZE, &len);
          if (!ret) {
            code = TSDB_CODE_SCALAR_CONVERT_ERROR;
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

    code = colDataSetVal(pOutput->columnData, i, output, false);
    if (TSDB_CODE_SUCCESS != code) {
      goto _end;
    }
  }

  pOutput->numOfRows = pInput->numOfRows;

_end:
  taosMemoryFreeClear(buf);
  taosMemoryFreeClear(output);
  taosMemoryFreeClear(convBuf);
  return code;
}

int32_t toISO8601Function(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  int32_t type = GET_PARAM_TYPE(pInput);

  bool    tzPresent = (inputNum == 2) ? true : false;
  char    tz[20] = {0};
  int32_t tzLen = 0;
  if (tzPresent) {
    tzLen = varDataLen(pInput[1].columnData->pData);
    (void)memcpy(tz, varDataVal(pInput[1].columnData->pData), tzLen);
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
    int32_t fractionLen;

    char    buf[TD_TIME_STR_LEN] = {0};
    int64_t timeVal;
    char*   format = NULL;
    int64_t  quot = 0;
    long    mod = 0;

    GET_TYPED_DATA(timeVal, int64_t, type, input);

    switch (pInput->columnData[0].info.precision) {
      case TSDB_TIME_PRECISION_MILLI: {
        quot = timeVal / 1000;
        fractionLen = 5;
        format = ".%03" PRId64;
        mod = timeVal % 1000;
        break;
      }

      case TSDB_TIME_PRECISION_MICRO: {
        quot = timeVal / 1000000;
        fractionLen = 8;
        format = ".%06" PRId64;
        mod = timeVal % 1000000;
        break;
      }

      case TSDB_TIME_PRECISION_NANO: {
        quot = timeVal / 1000000000;
        fractionLen = 11;
        format = ".%09" PRId64;
        mod = timeVal % 1000000000;
        break;
      }

      default: {
        colDataSetNULL(pOutput->columnData, i);
        continue;
      }
    }

    // trans current timezone's unix ts to dest timezone
    // offset = delta from dest timezone to zero
    // delta from zero to current timezone = 3600 * (cur)tsTimezone
    int64_t offset = 0;
    if (0 != offsetOfTimezone(tz, &offset)) {
      goto _end;
    }
    quot -= offset + 3600 * ((int64_t)tsTimezone);

    struct tm tmInfo;
    int32_t len = 0;

    if (taosLocalTime((const time_t *)&quot, &tmInfo, buf, sizeof(buf)) == NULL) {
      len = (int32_t)strlen(buf);
      goto _end;
    }

    len = (int32_t)strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%S", &tmInfo);

    len += tsnprintf(buf + len, fractionLen, format, mod);

    // add timezone string
    if (tzLen > 0) {
      (void)snprintf(buf + len, tzLen + 1, "%s", tz);
      len += tzLen;
    }

_end:
    memmove(buf + VARSTR_HEADER_SIZE, buf, len);
    varDataSetLen(buf, len);

    SCL_ERR_RET(colDataSetVal(pOutput->columnData, i, buf, false));
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
      SCL_ERR_RET(colDataSetVal(pOutput->columnData, i, (char *)&timeVal, false));
    }
  }

  pOutput->numOfRows = pInput->numOfRows;

  return TSDB_CODE_SUCCESS;
}

int32_t toJsonFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  int32_t type = GET_PARAM_TYPE(pInput);

  char tmp[TSDB_MAX_JSON_TAG_LEN] = {0};
  int32_t code = TSDB_CODE_SUCCESS;
  for (int32_t i = 0; i < pInput[0].numOfRows; ++i) {
    SArray *pTagVals = taosArrayInit(8, sizeof(STagVal));
    if (NULL == pTagVals) {
      return terrno;
    }
    STag   *pTag = NULL;

    if (colDataIsNull_s(pInput[0].columnData, i)) {
      code = tTagNew(pTagVals, 1, true, &pTag);
      if (TSDB_CODE_SUCCESS != code) {
        tTagFree(pTag);
        taosArrayDestroy(pTagVals);
        SCL_ERR_RET(code);
      }
    } else {
      char *input = pInput[0].columnData->pData + pInput[0].columnData->varmeta.offset[i];
      if (varDataLen(input) > (TSDB_MAX_JSON_TAG_LEN - VARSTR_HEADER_SIZE) / TSDB_NCHAR_SIZE) {
        taosArrayDestroy(pTagVals);
        return TSDB_CODE_FAILED;
      }
      (void)memcpy(tmp, varDataVal(input), varDataLen(input));
      tmp[varDataLen(input)] = 0;
      if (parseJsontoTagData(tmp, pTagVals, &pTag, NULL)) {
        code = tTagNew(pTagVals, 1, true, &pTag);
        if (TSDB_CODE_SUCCESS != code) {
          tTagFree(pTag);
          taosArrayDestroy(pTagVals);
          SCL_ERR_RET(code);
        }
      }
    }

    code = colDataSetVal(pOutput->columnData, i, (const char *)pTag, false);
    tTagFree(pTag);
    taosArrayDestroy(pTagVals);
    if (TSDB_CODE_SUCCESS != code ) {
      SCL_ERR_RET(code);
    }
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

  if (tsStr == NULL || format == NULL) {
    SCL_ERR_JRET(terrno);
  }
  for (int32_t i = 0; i < pInput[0].numOfRows; ++i) {
    if (colDataIsNull_s(pInput[1].columnData, i) || colDataIsNull_s(pInput[0].columnData, i)) {
      colDataSetNULL(pOutput->columnData, i);
      continue;
    }

    char *tsData = colDataGetData(pInput[0].columnData, i);
    char *formatData = colDataGetData(pInput[1].columnData, pInput[1].numOfRows > 1 ? i : 0);
    len = TMIN(TS_FORMAT_MAX_LEN - 1, varDataLen(tsData));
    (void)strncpy(tsStr, varDataVal(tsData), len);  // No need to handle the return value.
    tsStr[len] = '\0';
    len = TMIN(TS_FORMAT_MAX_LEN - 1, varDataLen(formatData));
    if (pInput[1].numOfRows > 1 || i == 0) {
      (void)strncpy(format, varDataVal(formatData), len);  // No need to handle the return value.
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
      SCL_ERR_JRET(code);
    }
    SCL_ERR_JRET(colDataSetVal(pOutput->columnData, i, (char *)&ts, false));
  }

_return:
  if (formats) taosArrayDestroy(formats);
  taosMemoryFreeClear(tsStr);
  taosMemoryFreeClear(format);
  return code;
}

int32_t toCharFunction(SScalarParam* pInput, int32_t inputNum, SScalarParam* pOutput) {
  char *  format = taosMemoryMalloc(TS_FORMAT_MAX_LEN);
  char *  out = taosMemoryCalloc(1, TS_FORMAT_MAX_LEN + VARSTR_HEADER_SIZE);
  int32_t len;
  SArray *formats = NULL;
  int32_t code = 0;

  if (format == NULL || out == NULL) {
    SCL_ERR_JRET(terrno);
  }
  for (int32_t i = 0; i < pInput[0].numOfRows; ++i) {
    if (colDataIsNull_s(pInput[1].columnData, i) || colDataIsNull_s(pInput[0].columnData, i)) {
      colDataSetNULL(pOutput->columnData, i);
      continue;
    }

    char *ts = colDataGetData(pInput[0].columnData, i);
    char *formatData = colDataGetData(pInput[1].columnData, pInput[1].numOfRows > 1 ? i : 0);
    len = TMIN(TS_FORMAT_MAX_LEN - VARSTR_HEADER_SIZE, varDataLen(formatData));
    if (pInput[1].numOfRows > 1 || i == 0) {
      (void)strncpy(format, varDataVal(formatData), len);
      format[len] = '\0';
      if (formats) {
        taosArrayDestroy(formats);
        formats = NULL;
      }
    }
    int32_t precision = pInput[0].columnData->info.precision;
    SCL_ERR_JRET(taosTs2Char(format, &formats, *(int64_t *)ts, precision, varDataVal(out), TS_FORMAT_MAX_LEN));
    varDataSetLen(out, strlen(varDataVal(out)));
    SCL_ERR_JRET(colDataSetVal(pOutput->columnData, i, out, false));
  }

_return:
  if (formats) taosArrayDestroy(formats);
  taosMemoryFreeClear(format);
  taosMemoryFreeClear(out);
  return code;
}

/** Time functions **/
int64_t offsetFromTz(char *timezone, int64_t factor) {
  char *minStr = &timezone[3];
  int64_t minutes = taosStr2Int64(minStr, NULL, 10);
  (void)memset(minStr, 0, strlen(minStr));
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
  (void)memcpy(timezone, varDataVal(pInput[timeZoneIdx].columnData->pData), varDataLen(pInput[timeZoneIdx].columnData->pData));

  for (int32_t i = 0; i < pInput[0].numOfRows; ++i) {
    if (colDataIsNull_s(pInput[0].columnData, i)) {
      colDataSetNULL(pOutput->columnData, i);
      continue;
    }

    char *input = colDataGetData(pInput[0].columnData, i);

    if (IS_VAR_DATA_TYPE(type)) { /* datetime format strings */
      int32_t ret = convertStringToTimestamp(type, input, timePrec, &timeVal);
      if (ret != TSDB_CODE_SUCCESS) {
        colDataSetNULL(pOutput->columnData, i);
        continue;
      }
    } else if (type == TSDB_DATA_TYPE_BIGINT) { /* unix timestamp */
      GET_TYPED_DATA(timeVal, int64_t, type, input);
    } else if (type == TSDB_DATA_TYPE_TIMESTAMP) { /* timestamp column*/
      GET_TYPED_DATA(timeVal, int64_t, type, input);
    }

    char buf[20] = {0};
    NUM_TO_STRING(TSDB_DATA_TYPE_BIGINT, &timeVal, sizeof(buf), buf);

    // truncate the timestamp to time_unit precision
    int64_t seconds = timeUnit / TSDB_TICK_PER_SECOND(timePrec);
    if (ignoreTz && (seconds == 604800 || seconds == 86400)) {
      timeVal = timeVal - (timeVal + offsetFromTz(timezone, TSDB_TICK_PER_SECOND(timePrec))) % timeUnit;
    } else {
      timeVal = timeVal / timeUnit * timeUnit;
    }
    SCL_ERR_RET(colDataSetVal(pOutput->columnData, i, (char *)&timeVal, false));
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

    int64_t result = timeVal[0] - timeVal[1];

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

    SCL_ERR_RET(colDataSetVal(pOutput->columnData, i, (char *)&result, false));
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

int32_t timeZoneStrLen() {
  return sizeof(VarDataLenT) + strlen(tsTimezoneStr);
}

int32_t timezoneFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  char output[TD_TIMEZONE_LEN + VARSTR_HEADER_SIZE] = {0};
  (void)memcpy(varDataVal(output), tsTimezoneStr, TD_TIMEZONE_LEN);
  varDataSetLen(output, strlen(tsTimezoneStr));
  for (int32_t i = 0; i < pInput->numOfRows; ++i) {
    SCL_ERR_RET(colDataSetVal(pOutput->columnData, i, output, false));
  }
  pOutput->numOfRows = pInput->numOfRows;
  return TSDB_CODE_SUCCESS;
}

int32_t weekdayFunctionImpl(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput, bool startFromZero) {
  int32_t type = GET_PARAM_TYPE(&pInput[0]);

  int64_t timePrec, timeVal = 0;

  GET_TYPED_DATA(timePrec, int64_t, GET_PARAM_TYPE(&pInput[1]), pInput[1].columnData->pData);

  for (int32_t i = 0; i < pInput[0].numOfRows; ++i) {
    if (colDataIsNull_s(pInput[0].columnData, i)) {
      colDataSetNULL(pOutput->columnData, i);
      continue;
    }

    char *input = colDataGetData(pInput[0].columnData, i);

    if (IS_VAR_DATA_TYPE(type)) { /* datetime format strings */
      int32_t ret = convertStringToTimestamp(type, input, timePrec, &timeVal);
      if (ret != TSDB_CODE_SUCCESS) {
        colDataSetNULL(pOutput->columnData, i);
        continue;
      }
    } else if (type == TSDB_DATA_TYPE_BIGINT) { /* unix timestamp */
      GET_TYPED_DATA(timeVal, int64_t, type, input);
    } else if (type == TSDB_DATA_TYPE_TIMESTAMP) { /* timestamp column*/
      GET_TYPED_DATA(timeVal, int64_t, type, input);
    }
    struct STm tm;
    TAOS_CHECK_RETURN(taosTs2Tm(timeVal, timePrec, &tm));
    int64_t ret = startFromZero ?  (tm.tm.tm_wday + 6) % 7 : tm.tm.tm_wday + 1;
    colDataSetInt64(pOutput->columnData, i, &ret);
  }

  pOutput->numOfRows = pInput->numOfRows;

  return TSDB_CODE_SUCCESS;
}

int32_t weekdayFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  return weekdayFunctionImpl(pInput, inputNum, pOutput, true);
}

int32_t dayofweekFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  return weekdayFunctionImpl(pInput, inputNum, pOutput, false);
}

// calculate day number from 0000-00-00
static int32_t getDayNum(int32_t year, int32_t month, int32_t day) {
  int32_t delsum;
  int32_t temp;
  int32_t y = year;

  if (y == 0 && month == 0) {
    return 0;
  }

  delsum = 365 * y + 31 * (month - 1) + day;
  if (month <= 2)
    y--;
  else
    delsum -= (month * 4 + 23) / 10;
  temp = ((y / 100 + 1) * 3) / 4;
  return (delsum + y / 4 - temp);
}

static int32_t getDaysInYear(int32_t year) {
  return (((year % 100) == 0) ? ((year % 400) == 0) : ((year % 4) == 0)) ? 366 : 365;
}

static int32_t getWeekday(int32_t daynr, bool sundayFirstDay) {
  return (daynr + 5 + (sundayFirstDay ? 1 : 0)) % 7;
}

static int32_t calculateWeekNum(struct tm date, int32_t weekFlag) {
  int32_t days;
  int32_t year = date.tm_year + 1900;
  int32_t month = date.tm_mon + 1;
  int32_t day = date.tm_mday;
  int32_t dayNum = getDayNum(year, month, day);
  int32_t firstDayNum = getDayNum(year, 1, 1);
  bool mondayFirst = (weekFlag & WEEK_FLAG_MONDAY_FIRST);
  bool weekStartFromOne = (weekFlag & WEEK_FLAG_FROM_ONE);
  bool firstWeekday = (weekFlag & WEEK_FLAG_INCLUDE_FIRST_DAY);

  int32_t weekday = getWeekday(firstDayNum, !mondayFirst);
  if (month == 1 && day <= 7 - weekday) {
    if (!weekStartFromOne && ((firstWeekday && weekday != 0) || (!firstWeekday && weekday >= 4))) {
      return 0;
    }
    weekStartFromOne = true;
    days = getDaysInYear(--year);
    firstDayNum -= days;
    weekday = (weekday + 53 * 7 - days) % 7;
  }

  if ((firstWeekday && weekday != 0) || (!firstWeekday && weekday >= 4))
    days = dayNum - (firstDayNum + (7 - weekday));
  else
    days = dayNum - (firstDayNum - weekday);

  if (weekStartFromOne && days >= 52 * 7) {
    weekday = (weekday + getDaysInYear(year)) % 7;
    if ((!firstWeekday && weekday < 4) || (firstWeekday && weekday == 0)) {
      return 1;
    }
  }
  return days / 7 + 1;
}

static int32_t weekMode(int32_t mode) {
  return mode & WEEK_FLAG_MONDAY_FIRST ? mode : mode ^ WEEK_FLAG_INCLUDE_FIRST_DAY;
}

int32_t weekFunctionImpl(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput, int64_t prec, int32_t mode) {
  int32_t type = GET_PARAM_TYPE(&pInput[0]);

  int64_t timeVal = 0;

  for (int32_t i = 0; i < pInput[0].numOfRows; ++i) {
    if (colDataIsNull_s(pInput[0].columnData, i)) {
      colDataSetNULL(pOutput->columnData, i);
      continue;
    }

    char *input = colDataGetData(pInput[0].columnData, i);

    if (IS_VAR_DATA_TYPE(type)) { /* datetime format strings */
      int32_t ret = convertStringToTimestamp(type, input, prec, &timeVal);
      if (ret != TSDB_CODE_SUCCESS) {
        colDataSetNULL(pOutput->columnData, i);
        continue;
      }
    } else if (type == TSDB_DATA_TYPE_BIGINT) { /* unix timestamp */
      GET_TYPED_DATA(timeVal, int64_t, type, input);
    } else if (type == TSDB_DATA_TYPE_TIMESTAMP) { /* timestamp column*/
      GET_TYPED_DATA(timeVal, int64_t, type, input);
    }
    struct STm tm;
    SCL_ERR_RET(taosTs2Tm(timeVal, prec, &tm));
    int64_t ret = calculateWeekNum(tm.tm, weekMode(mode));
    colDataSetInt64(pOutput->columnData, i, &ret);
  }

  pOutput->numOfRows = pInput->numOfRows;

  return TSDB_CODE_SUCCESS;
}

int32_t weekFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  int64_t timePrec;
  int32_t mode = 0;
  if (inputNum == 2) {
    GET_TYPED_DATA(timePrec, int64_t, GET_PARAM_TYPE(&pInput[1]), pInput[1].columnData->pData);
    return weekFunctionImpl(pInput, inputNum, pOutput, timePrec, mode);
  } else {
    GET_TYPED_DATA(mode, int32_t , GET_PARAM_TYPE(&pInput[1]), pInput[1].columnData->pData);
    GET_TYPED_DATA(timePrec, int64_t, GET_PARAM_TYPE(&pInput[2]), pInput[2].columnData->pData);
    return weekFunctionImpl(pInput, inputNum, pOutput, timePrec, mode);
  }
}

int32_t weekofyearFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  int64_t timePrec;
  GET_TYPED_DATA(timePrec, int64_t, GET_PARAM_TYPE(&pInput[1]), pInput[1].columnData->pData);
  return weekFunctionImpl(pInput, inputNum, pOutput, timePrec, 3);
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

int32_t randFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  int32_t seed;
  int32_t numOfRows = inputNum == 1 ? pInput[0].numOfRows : TMAX(pInput[0].numOfRows, pInput[1].numOfRows);
  for (int32_t i = 0; i < numOfRows; ++i) {
    // for constant seed, only set seed once
    if ((pInput[0].numOfRows == 1 && i == 0) || (pInput[0].numOfRows != 1)) {
      if (!IS_NULL_TYPE(GET_PARAM_TYPE(&pInput[0])) && !colDataIsNull_s(pInput[0].columnData, i)) {
        GET_TYPED_DATA(seed, int32_t, GET_PARAM_TYPE(&pInput[0]), colDataGetData(pInput[0].columnData, i));
        taosSeedRand(seed);
      }
    }
    double random_value = (double)(taosRand() % RAND_MAX) / RAND_MAX;
    colDataSetDouble(pOutput->columnData, i, &random_value);
  }
  pOutput->numOfRows = numOfRows;
  return TSDB_CODE_SUCCESS;
}

static double decimalFn(double val1, double val2, _double_fn fn) {
  if (val1 > DBL_MAX || val1 < -DBL_MAX) {
    return val1;
  }

  double scale = pow(10, val2);

  if (val2 < 0) {
    return fn(val1 * scale) / scale;
  } else {
    double scaled_number = val1 * scale;

    if (scaled_number > DBL_MAX || scaled_number < -DBL_MAX) {
      return val1;
    }

    return fn(scaled_number) / scale;
  }
}

static float decimalfFn(float val1, float val2, _float_fn fn) {
  if (val1 > FLT_MAX || val1 < -FLT_MAX) {
    return val1;
  }

  float scale = powf(10, val2);

  if (val2 < 0) {
    return fn(val1 * scale) / scale;
  } else {
    float scaled_number = val1 * scale;

    if (scaled_number > FLT_MAX || scaled_number < -FLT_MAX) {
      return val1;
    }

    return fn(scaled_number) / scale;
  }
}

static double roundFn(double val1, double val2) {
  return decimalFn(val1, val2, round);
}

static float roundfFn(float val1, float val2) {
  return decimalfFn(val1, val2, roundf);
}

static double truncFn(double val1, double val2) {
  return decimalFn(val1, val2, trunc);
}

static float truncfFn(float val1, float val2) {
  return decimalfFn(val1, val2, truncf);
}

static int32_t doScalarFunction2(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput, _float_fn_2 f1,
                                 _double_fn_2 d1) {
  SColumnInfoData     *pInputData[2];
  SColumnInfoData     *pOutputData = pOutput->columnData;
  _getDoubleValue_fn_t getValueFn[2];

  for (int32_t i = 0; i < inputNum; ++i) {
    pInputData[i] = pInput[i].columnData;
    SCL_ERR_RET(getVectorDoubleValueFn(GET_PARAM_TYPE(&pInput[i]), &getValueFn[i]));
  }

  bool hasNullType = (IS_NULL_TYPE(GET_PARAM_TYPE(&pInput[0])) || IS_NULL_TYPE(GET_PARAM_TYPE(&pInput[1])));

  int32_t numOfRows = TMAX(pInput[0].numOfRows, pInput[1].numOfRows);
  if (pInput[0].numOfRows == pInput[1].numOfRows) {
    for (int32_t i = 0; i < numOfRows; ++i) {
      if (colDataIsNull_s(pInputData[0], i) || colDataIsNull_s(pInputData[1], i) || hasNullType) {
        colDataSetNULL(pOutputData, i);
        continue;
      }
      double in2;
      GET_TYPED_DATA(in2, double, GET_PARAM_TYPE(&pInput[1]), colDataGetData(pInputData[1], i));
      switch (GET_PARAM_TYPE(&pInput[0])) {
        case TSDB_DATA_TYPE_DOUBLE: {
          double  *in = (double *)pInputData[0]->pData;
          double  *out = (double *)pOutputData->pData;
          double  result = d1(in[i], in2);
          if (isinf(result) || isnan(result)) {
            colDataSetNULL(pOutputData, i);
          } else {
            out[i] = result;
          }
          break;
        }
        case TSDB_DATA_TYPE_FLOAT: {
          float   *in = (float *)pInputData[0]->pData;
          float   *out = (float *)pOutputData->pData;
          float   result = f1(in[i], (float)in2);
          if (isinf(result) || isnan(result)) {
            colDataSetNULL(pOutputData, i);
          } else {
            out[i] = result;
          }
          break;
        }
        case TSDB_DATA_TYPE_TINYINT: {
          int8_t *in = (int8_t *)pInputData[0]->pData;
          int8_t *out = (int8_t *)pOutputData->pData;
          int8_t result = (int8_t)d1((double)in[i], in2);
          out[i] = result;
          break;
        }
        case TSDB_DATA_TYPE_SMALLINT: {
          int16_t *in = (int16_t *)pInputData[0]->pData;
          int16_t *out = (int16_t *)pOutputData->pData;
          int16_t result = (int16_t)d1((double)in[i], in2);
          out[i] = result;
          break;
        }
        case TSDB_DATA_TYPE_INT: {
          int32_t *in = (int32_t *)pInputData[0]->pData;
          int32_t *out = (int32_t *)pOutputData->pData;
          int32_t result = (int32_t)d1((double)in[i], in2);
          out[i] = result;
          break;
        }
        case TSDB_DATA_TYPE_BIGINT: {
          int64_t *in = (int64_t *)pInputData[0]->pData;
          int64_t *out = (int64_t *)pOutputData->pData;
          int64_t result = (int64_t)d1((double)in[i], in2);
          out[i] = result;
          break;
        }
        case TSDB_DATA_TYPE_UTINYINT: {
          uint8_t *in = (uint8_t *)pInputData[0]->pData;
          uint8_t *out = (uint8_t *)pOutputData->pData;
          uint8_t result = (uint8_t)d1((double)in[i], in2);
          out[i] = result;
          break;
        }
        case TSDB_DATA_TYPE_USMALLINT: {
          uint16_t *in = (uint16_t *)pInputData[0]->pData;
          uint16_t *out = (uint16_t *)pOutputData->pData;
          uint16_t result = (uint16_t)d1((double)in[i], in2);
          out[i] = result;
          break;
        }
        case TSDB_DATA_TYPE_UINT: {
          uint32_t *in = (uint32_t *)pInputData[0]->pData;
          uint32_t *out = (uint32_t *)pOutputData->pData;
          uint32_t result = (uint32_t)d1((double)in[i], in2);
          out[i] = result;
          break;
        }
        case TSDB_DATA_TYPE_UBIGINT: {
          uint64_t *in = (uint64_t *)pInputData[0]->pData;
          uint64_t *out = (uint64_t *)pOutputData->pData;
          uint64_t result = (uint64_t)d1((double)in[i], in2);
          out[i] = result;
          break;
        }
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
        double in2;
        GET_TYPED_DATA(in2, double, GET_PARAM_TYPE(&pInput[1]), colDataGetData(pInputData[1], i));
        switch (GET_PARAM_TYPE(&pInput[0])) {
          case TSDB_DATA_TYPE_DOUBLE: {
            double   *in = (double *)pInputData[0]->pData;
            double   *out = (double *)pOutputData->pData;
            double   result = d1(in[0], in2);
            if (isinf(result) || isnan(result)) {
              colDataSetNULL(pOutputData, i);
            } else {
              out[i] = result;
            }
            break;
          }
          case TSDB_DATA_TYPE_FLOAT: {
            float   *in = (float *)pInputData[0]->pData;
            float   *out = (float *)pOutputData->pData;
            float   result = f1(in[0], (float)in2);
            if (isinf(result) || isnan(result)) {
              colDataSetNULL(pOutputData, i);
            } else {
              out[i] = result;
            }
            break;
          }
          case TSDB_DATA_TYPE_TINYINT: {
            int8_t *in = (int8_t *)pInputData[0]->pData;
            int8_t *out = (int8_t *)pOutputData->pData;
            int8_t result = (int8_t)d1((double)in[0], in2);
            out[i] = result;
            break;
          }
          case TSDB_DATA_TYPE_SMALLINT: {
            int16_t *in = (int16_t *)pInputData[0]->pData;
            int16_t *out = (int16_t *)pOutputData->pData;
            int16_t result = (int16_t)d1((double)in[0], in2);
            out[i] = result;
            break;
          }
          case TSDB_DATA_TYPE_INT: {
            int32_t *in = (int32_t *)pInputData[0]->pData;
            int32_t *out = (int32_t *)pOutputData->pData;
            int32_t result = (int32_t)d1((double)in[0], in2);
            out[i] = result;
            break;
          }
          case TSDB_DATA_TYPE_BIGINT: {
            int64_t *in = (int64_t *)pInputData[0]->pData;
            int64_t *out = (int64_t *)pOutputData->pData;
            int64_t result = (int64_t)d1((double)in[0], in2);
            out[i] = result;
            break;
          }
          case TSDB_DATA_TYPE_UTINYINT: {
            uint8_t *in = (uint8_t *)pInputData[0]->pData;
            uint8_t *out = (uint8_t *)pOutputData->pData;
            uint8_t result = (uint8_t)d1((double)in[0], in2);
            out[i] = result;
            break;
          }
          case TSDB_DATA_TYPE_USMALLINT: {
            uint16_t *in = (uint16_t *)pInputData[0]->pData;
            uint16_t *out = (uint16_t *)pOutputData->pData;
            uint16_t result = (uint16_t)d1((double)in[0], in2);
            out[i] = result;
            break;
          }
          case TSDB_DATA_TYPE_UINT: {
            uint32_t *in = (uint32_t *)pInputData[0]->pData;
            uint32_t *out = (uint32_t *)pOutputData->pData;
            uint32_t result = (uint32_t)d1((double)in[0], in2);
            out[i] = result;
            break;
          }
          case TSDB_DATA_TYPE_UBIGINT: {
            uint64_t *in = (uint64_t *)pInputData[0]->pData;
            uint64_t *out = (uint64_t *)pOutputData->pData;
            uint64_t result = (uint64_t)d1((double)in[0], in2);
            out[i] = result;
            break;
          }
        }
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
        double in2;
        GET_TYPED_DATA(in2, double, GET_PARAM_TYPE(&pInput[1]), colDataGetData(pInputData[1], 0));
        switch (GET_PARAM_TYPE(&pInput[0])) {
          case TSDB_DATA_TYPE_DOUBLE: {
            double   *in = (double *)pInputData[0]->pData;
            double   *out = (double *)pOutputData->pData;
            double   result = d1(in[i], in2);
            if (isinf(result) || isnan(result)) {
              colDataSetNULL(pOutputData, i);
            } else {
              out[i] = result;
            }
            break;
          }
          case TSDB_DATA_TYPE_FLOAT: {
            float   *in = (float *)pInputData[0]->pData;
            float   *out = (float *)pOutputData->pData;
            float   result = f1(in[i], in2);
            if (isinf(result) || isnan(result)) {
              colDataSetNULL(pOutputData, i);
            } else {
              out[i] = result;
            }
            break;
          }
          case TSDB_DATA_TYPE_TINYINT: {
            int8_t *in = (int8_t *)pInputData[0]->pData;
            int8_t *out = (int8_t *)pOutputData->pData;
            int8_t result = (int8_t)d1((double)in[i], in2);
            out[i] = result;
            break;
          }
          case TSDB_DATA_TYPE_SMALLINT: {
            int16_t *in = (int16_t *)pInputData[0]->pData;
            int16_t *out = (int16_t *)pOutputData->pData;
            int16_t result = (int16_t)d1((double)in[i], in2);
            out[i] = result;
            break;
          }
          case TSDB_DATA_TYPE_INT: {
            int32_t *in = (int32_t *)pInputData[0]->pData;
            int32_t *out = (int32_t *)pOutputData->pData;
            int32_t result = (int32_t)d1((double)in[i], in2);
            out[i] = result;
            break;
          }
          case TSDB_DATA_TYPE_BIGINT: {
            int64_t *in = (int64_t *)pInputData[0]->pData;
            int64_t *out = (int64_t *)pOutputData->pData;
            int64_t result = (int64_t)d1((double)in[i], in2);
            out[i] = result;
            break;
          }
          case TSDB_DATA_TYPE_UTINYINT: {
            uint8_t *in = (uint8_t *)pInputData[0]->pData;
            uint8_t *out = (uint8_t *)pOutputData->pData;
            uint8_t result = (uint8_t)d1((double)in[i], in2);
            out[i] = result;
            break;
          }
          case TSDB_DATA_TYPE_USMALLINT: {
            uint16_t *in = (uint16_t *)pInputData[0]->pData;
            uint16_t *out = (uint16_t *)pOutputData->pData;
            uint16_t result = (uint16_t)d1((double)in[i], in2);
            out[i] = result;
            break;
          }
          case TSDB_DATA_TYPE_UINT: {
            uint32_t *in = (uint32_t *)pInputData[0]->pData;
            uint32_t *out = (uint32_t *)pOutputData->pData;
            uint32_t result = (uint32_t)d1((double)in[i], in2);
            out[i] = result;
            break;
          }
          case TSDB_DATA_TYPE_UBIGINT: {
            uint64_t *in = (uint64_t *)pInputData[0]->pData;
            uint64_t *out = (uint64_t *)pOutputData->pData;
            uint64_t result = (uint64_t)d1((double)in[i], in2);
            out[i] = result;
            break;
          }
        }
      }
    }
  }

  pOutput->numOfRows = numOfRows;
  return TSDB_CODE_SUCCESS;
}

int32_t roundFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  if (inputNum == 1) {
    return doScalarFunction(pInput, inputNum, pOutput, roundf, round);
  } else {
    return doScalarFunction2(pInput, inputNum, pOutput, roundfFn, roundFn);
  }
}

int32_t truncFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  return doScalarFunction2(pInput, inputNum, pOutput, truncfFn, truncFn);
}

int32_t piFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  double_t value = M_PI;
  for (int32_t i = 0; i < pInput->numOfRows; ++i) {
    colDataSetDouble(pOutput->columnData, i, &value);
  }
  pOutput->numOfRows = pInput->numOfRows;
  return TSDB_CODE_SUCCESS;
}

int32_t expFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  return doScalarFunctionUnique(pInput, inputNum, pOutput, exp);
}

int32_t lnFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  return doScalarFunctionUnique(pInput, inputNum, pOutput, tlog);
}

int32_t modFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  return doScalarFunctionUnique2(pInput, inputNum, pOutput, fmod);
}

int32_t degreesFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  return doScalarFunctionUnique(pInput, inputNum, pOutput, degrees);
}

int32_t radiansFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  return doScalarFunctionUnique(pInput, inputNum, pOutput, radians);
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
  return doTrimFunction(pInput, inputNum, pOutput, tltrimspace, tltrim);
}

int32_t rtrimFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  return doTrimFunction(pInput, inputNum, pOutput, trtrimspace, tltrim);
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

int32_t stdScalarFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
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
    int  n = tsnprintf(slopBuf, 64, "%.6lf", matrix02);
    if (n > LEASTSQUARES_DOUBLE_ITEM_LENGTH) {
      (void)snprintf(slopBuf, 64, "%." DOUBLE_PRECISION_DIGITS, matrix02);
    }
    n = tsnprintf(interceptBuf, 64, "%.6lf", matrix12);
    if (n > LEASTSQUARES_DOUBLE_ITEM_LENGTH) {
      (void) snprintf(interceptBuf, 64, "%." DOUBLE_PRECISION_DIGITS, matrix12);
    }
    size_t len =
        snprintf(varDataVal(buf), sizeof(buf) - VARSTR_HEADER_SIZE, "{slop:%s, intercept:%s}", slopBuf, interceptBuf);
    varDataSetLen(buf, len);
    SCL_ERR_RET(colDataSetVal(pOutputData, 0, buf, false));
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
    SCL_ERR_RET(colDataSetVal(pOutputData, 0, (char *)&val, false));
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
    SCL_ERR_RET(colDataSetVal(pOutputData, 0, (char *)&result, false));
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

int32_t forecastScalarFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
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
    SCL_ERR_RET(colDataSetVal(pOutputData, i, (char *)&out, false));
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
    SCL_ERR_RET(colDataSetVal(pOutputData, i, (char *)&out, false));
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

static int32_t getHistogramBinDesc(SHistoFuncBin **bins, int32_t *binNum, char *binDescStr, int8_t binType,
                                bool normalized) {
  cJSON  *binDesc = cJSON_Parse(binDescStr);
  int32_t numOfBins;
  double *intervals;
  if (cJSON_IsObject(binDesc)) { /* linaer/log bins */
    int32_t numOfParams = cJSON_GetArraySize(binDesc);
    int32_t startIndex;
    if (numOfParams != 4) {
      cJSON_Delete(binDesc);
      SCL_ERR_RET(TSDB_CODE_FAILED);
    }

    cJSON *start = cJSON_GetObjectItem(binDesc, "start");
    cJSON *factor = cJSON_GetObjectItem(binDesc, "factor");
    cJSON *width = cJSON_GetObjectItem(binDesc, "width");
    cJSON *count = cJSON_GetObjectItem(binDesc, "count");
    cJSON *infinity = cJSON_GetObjectItem(binDesc, "infinity");

    if (!cJSON_IsNumber(start) || !cJSON_IsNumber(count) || !cJSON_IsBool(infinity)) {
      cJSON_Delete(binDesc);
      SCL_RET(TSDB_CODE_SUCCESS);
    }

    if (count->valueint <= 0 || count->valueint > 1000) {  // limit count to 1000
      cJSON_Delete(binDesc);
      SCL_ERR_RET(TSDB_CODE_FAILED);
    }

    if (isinf(start->valuedouble) || (width != NULL && isinf(width->valuedouble)) ||
        (factor != NULL && isinf(factor->valuedouble)) || (count != NULL && isinf(count->valuedouble))) {
      cJSON_Delete(binDesc);
      SCL_ERR_RET(TSDB_CODE_FAILED);
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
    if (NULL == intervals) {
      cJSON_Delete(binDesc);
      SCL_ERR_RET(terrno);
    }
    if (cJSON_IsNumber(width) && factor == NULL && binType == LINEAR_BIN) {
      // linear bin process
      if (width->valuedouble == 0) {
        taosMemoryFreeClear(intervals);
        cJSON_Delete(binDesc);
        SCL_ERR_RET(TSDB_CODE_FAILED);
      }
      for (int i = 0; i < counter + 1; ++i) {
        intervals[startIndex] = start->valuedouble + i * width->valuedouble;
        if (isinf(intervals[startIndex])) {
          taosMemoryFreeClear(intervals);
          cJSON_Delete(binDesc);
          SCL_ERR_RET(TSDB_CODE_FAILED);
        }
        startIndex++;
      }
    } else if (cJSON_IsNumber(factor) && width == NULL && binType == LOG_BIN) {
      // log bin process
      if (start->valuedouble == 0) {
        taosMemoryFreeClear(intervals);
        cJSON_Delete(binDesc);
        SCL_ERR_RET(TSDB_CODE_FAILED);
      }
      if (factor->valuedouble < 0 || factor->valuedouble == 0 || factor->valuedouble == 1) {
        taosMemoryFreeClear(intervals);
        cJSON_Delete(binDesc);
        SCL_ERR_RET(TSDB_CODE_FAILED);
      }
      for (int i = 0; i < counter + 1; ++i) {
        intervals[startIndex] = start->valuedouble * pow(factor->valuedouble, i * 1.0);
        if (isinf(intervals[startIndex])) {
          taosMemoryFreeClear(intervals);
          cJSON_Delete(binDesc);
          SCL_ERR_RET(TSDB_CODE_FAILED);
        }
        startIndex++;
      }
    } else {
      taosMemoryFreeClear(intervals);
      cJSON_Delete(binDesc);
      SCL_ERR_RET(TSDB_CODE_FAILED);
    }

    if (infinity->valueint == true) {
      intervals[0] = -INFINITY;
      intervals[numOfBins - 1] = INFINITY;
      // in case of desc bin orders, -inf/inf should be swapped
      if (numOfBins < 4) {
        SCL_ERR_RET(TSDB_CODE_FAILED);
      }
      if (intervals[1] > intervals[numOfBins - 2]) {
        TSWAP(intervals[0], intervals[numOfBins - 1]);
      }
    }
  } else if (cJSON_IsArray(binDesc)) { /* user input bins */
    if (binType != USER_INPUT_BIN) {
      cJSON_Delete(binDesc);
      SCL_ERR_RET(TSDB_CODE_FAILED);
    }
    numOfBins = cJSON_GetArraySize(binDesc);
    intervals = taosMemoryCalloc(numOfBins, sizeof(double));
    if (NULL == intervals) {
      cJSON_Delete(binDesc);
      SCL_ERR_RET(terrno);
    }
    cJSON *bin = binDesc->child;
    if (bin == NULL) {
      taosMemoryFreeClear(intervals);
      cJSON_Delete(binDesc);
      SCL_ERR_RET(TSDB_CODE_FAILED);
    }
    int i = 0;
    while (bin) {
      intervals[i] = bin->valuedouble;
      if (!cJSON_IsNumber(bin)) {
        taosMemoryFreeClear(intervals);
        cJSON_Delete(binDesc);
        SCL_ERR_RET(TSDB_CODE_FAILED);
      }
      if (i != 0 && intervals[i] <= intervals[i - 1]) {
        taosMemoryFreeClear(intervals);
        cJSON_Delete(binDesc);
        SCL_ERR_RET(TSDB_CODE_FAILED);
      }
      bin = bin->next;
      i++;
    }
  } else {
    cJSON_Delete(binDesc);
    SCL_RET(TSDB_CODE_FAILED);
  }

  *binNum = numOfBins - 1;
  *bins = taosMemoryCalloc(numOfBins, sizeof(SHistoFuncBin));
  if (NULL == bins) {
    SCL_ERR_RET(terrno);
  }
  for (int32_t i = 0; i < *binNum; ++i) {
    (*bins)[i].lower = intervals[i] < intervals[i + 1] ? intervals[i] : intervals[i + 1];
    (*bins)[i].upper = intervals[i + 1] > intervals[i] ? intervals[i + 1] : intervals[i];
    (*bins)[i].count = 0;
  }

  taosMemoryFreeClear(intervals);
  cJSON_Delete(binDesc);

  SCL_RET(TSDB_CODE_SUCCESS);
}

int32_t histogramScalarFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  SColumnInfoData *pInputData = pInput->columnData;
  SColumnInfoData *pOutputData = pOutput->columnData;

  SHistoFuncBin *bins;
  int32_t        numOfBins = 0;
  int32_t        totalCount = 0;

  char *binTypeStr = taosStrndup(varDataVal(pInput[1].columnData->pData), varDataLen(pInput[1].columnData->pData));
  if (NULL == binTypeStr) {
    SCL_ERR_RET(terrno);
  }
  int8_t binType = getHistogramBinType(binTypeStr);
  taosMemoryFreeClear(binTypeStr);

  char   *binDesc = taosStrndup(varDataVal(pInput[2].columnData->pData), varDataLen(pInput[2].columnData->pData));
  if (NULL == binDesc) {
    SCL_ERR_RET(terrno);
  }
  int64_t normalized = *(int64_t *)(pInput[3].columnData->pData);

  int32_t type = GET_PARAM_TYPE(pInput);
  int32_t code = getHistogramBinDesc(&bins, &numOfBins, binDesc, binType, (bool)normalized);
  if (TSDB_CODE_SUCCESS != code) {
    taosMemoryFreeClear(binDesc);
    return code;
  }
  taosMemoryFreeClear(binDesc);

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

  SCL_ERR_JRET(colInfoDataEnsureCapacity(pOutputData, numOfBins, false));

  for (int32_t k = 0; k < numOfBins; ++k) {
    int32_t len;
    char    buf[512] = {0};
    if (!normalized) {
      len = tsnprintf(varDataVal(buf), sizeof(buf) - VARSTR_HEADER_SIZE, "{\"lower_bin\":%g, \"upper_bin\":%g, \"count\":%" PRId64 "}",
                     bins[k].lower, bins[k].upper, bins[k].count);
    } else {
      len = tsnprintf(varDataVal(buf), sizeof(buf) - VARSTR_HEADER_SIZE, "{\"lower_bin\":%g, \"upper_bin\":%g, \"count\":%lf}",
                     bins[k].lower, bins[k].upper, bins[k].percentage);
    }
    varDataSetLen(buf, len);
    SCL_ERR_JRET(colDataSetVal(pOutputData, k, buf, false));
  }
  pOutput->numOfRows = numOfBins;

_return:
  taosMemoryFreeClear(bins);
  return code;
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
    SCL_ERR_RET(colDataSetVal(pOutputData, i, data, false));
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
