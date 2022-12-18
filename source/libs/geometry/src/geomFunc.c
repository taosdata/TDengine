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

#include <geos_c.h>
#include "geosWrapper.h"
#include "geomFunc.h"
#include "querynodes.h"
#include "tdatablock.h"
#include "sclInt.h"
#include "sclvector.h"

typedef int32_t (*_geomPrepareFunc_fn_t)(SGeosContext* context);
typedef int32_t (*_geomExecuteOneParamFunc_fn_t)(SGeosContext* context, SColumnInfoData *pInputData,
                                                 int32_t i, SColumnInfoData *pOutputData);
typedef int32_t (*_geomExecuteTwoParamsFunc_fn_t)(SGeosContext* context, SColumnInfoData *pInputData[],
                                                  int32_t iLeft, int32_t iRight, SColumnInfoData *pOutputData);

// both input and output are with VARSTR format
// need to call taosMemoryFree(*output) later
int32_t doGeomFromTextFunc(SGeosContext *context, const char *input, unsigned char **output) {
  int32_t code = TSDB_CODE_FAILED;

  if ((varDataLen(input)) == 0) { //empty value
    *output = NULL;
    return TSDB_CODE_SUCCESS;
  }

  //make input as a zero ending string
  char *end = varDataVal(input) + varDataLen(input);
  char endValue = *end;
  *end = 0; 

  unsigned char *outputGeom = NULL;
  size_t size = 0;

  code = doGeomFromText(context, varDataVal(input), &outputGeom, &size);
  if (code != TSDB_CODE_SUCCESS) {
    goto _exit;
  }

  *output = taosMemoryCalloc(1, size + VARSTR_HEADER_SIZE);
  if (*output == NULL) {
    code = TSDB_CODE_TSC_OUT_OF_MEMORY;
    goto _exit;
  }

  memcpy(varDataVal(*output), outputGeom, size);
  varDataSetLen(*output, size);
  code = TSDB_CODE_SUCCESS;

_exit:
  if (outputGeom) {
    GEOSFree_r(context->handle, outputGeom);
    outputGeom = NULL;
  }

  *end = endValue;  //recover the input string

  return code;
}

// both input and output are with VARSTR format
// need to call taosMemoryFree(*output) later
int32_t doAsTextFunc(SGeosContext *context, unsigned char *input, char **output) {
  int32_t code = TSDB_CODE_FAILED;

  if ((varDataLen(input)) == 0) { //empty value
    *output = NULL;
    return TSDB_CODE_SUCCESS;
  }

  char *outputWKT = NULL;
  code = doAsText(context, varDataVal(input), varDataLen(input), &outputWKT);
  if (code != TSDB_CODE_SUCCESS) {
    goto _exit;
  }

  size_t size = strlen(outputWKT);
  *output = taosMemoryCalloc(1, size + VARSTR_HEADER_SIZE);
  if (*output == NULL) {
    code = TSDB_CODE_TSC_OUT_OF_MEMORY;
    goto _exit;
  }

  memcpy(varDataVal(*output), outputWKT, size);
  varDataSetLen(*output, size);
  code = TSDB_CODE_SUCCESS;

_exit:
  if (outputWKT) {
    GEOSFree_r(context->handle, outputWKT);
    outputWKT = NULL;
  }

  return code;
}

// output is with VARSTR format
// need to call taosMemoryFree(*output) later
int32_t doMakePointFunc(SGeosContext *context, double x, double y, unsigned char **output) {
  int32_t code = TSDB_CODE_FAILED;

  unsigned char *outputGeom = NULL;
  size_t size = 0;
  code = doMakePoint(context, x, y, &outputGeom, &size);
  if (code != TSDB_CODE_SUCCESS) {
    goto _exit;
  }

  *output = taosMemoryCalloc(1, size + VARSTR_HEADER_SIZE);
  if (*output == NULL) {
    code = TSDB_CODE_TSC_OUT_OF_MEMORY;
    goto _exit;
  }

  memcpy(varDataVal(*output), outputGeom, size);
  varDataSetLen(*output, size);
  code = TSDB_CODE_SUCCESS;

_exit:
  if (outputGeom) {
    GEOSFree_r(context->handle, outputGeom);
    outputGeom = NULL;
  }

  return code;
}

// both input and output are with VARSTR format
int32_t doIntersectsFunc(SGeosContext *context, unsigned char *input1, unsigned char *input2, char *res) {
  int32_t code = TSDB_CODE_FAILED;

  if (varDataLen(input1) == 0 || varDataLen(input2) == 0) { //empty value
    *res = -1;  // it means a NULL result
    return TSDB_CODE_SUCCESS;
  }

  code = doIntersects(context,
                      varDataVal(input1), varDataLen(input1),
                      varDataVal(input2), varDataLen(input2),
                      res);

  return code;
}

int32_t executeGeomFromTextFunc(SGeosContext* context, SColumnInfoData *pInputData,
                                int32_t i, SColumnInfoData *pOutputData) {
  int32_t code = TSDB_CODE_FAILED;

  char *input = colDataGetData(pInputData, i);
  unsigned char *output = NULL;
  code = doGeomFromTextFunc(context, input, &output);
  if (code != TSDB_CODE_SUCCESS) {
    goto _exit;
  }

  colDataAppend(pOutputData, i, output, (output == NULL));

_exit:
  if (output) {
    taosMemoryFree(output);
  }

  return code;
}

int32_t executeAsTextFunc(SGeosContext* context, SColumnInfoData *pInputData,
                          int32_t i, SColumnInfoData *pOutputData) {
  int32_t code = TSDB_CODE_FAILED;

  unsigned char *input = colDataGetData(pInputData, i);
  char *output = NULL;
  code = doAsTextFunc(context, input, &output);
  if (code != TSDB_CODE_SUCCESS) {
    goto _exit;
  }

  colDataAppend(pOutputData, i, output, (output == NULL));

_exit:
  if (output) {
    taosMemoryFree(output);
  }

  return code;
}

int32_t executeMakePointFunc(SGeosContext* context, SColumnInfoData *pInputData[],
                             int32_t iLeft, int32_t iRight, SColumnInfoData *pOutputData) {
  int32_t code = TSDB_CODE_FAILED;

  _getDoubleValue_fn_t getDoubleValueFn[2];
  getDoubleValueFn[0]= getVectorDoubleValueFn(pInputData[0]->info.type);
  getDoubleValueFn[1]= getVectorDoubleValueFn(pInputData[1]->info.type);

  unsigned char *output = NULL;
  code = doMakePointFunc(context, getDoubleValueFn[0](pInputData[0]->pData, iLeft), getDoubleValueFn[1](pInputData[1]->pData, iRight), &output);
  if (code != TSDB_CODE_SUCCESS) {
    goto _exit;
  }

  colDataAppend(pOutputData, TMAX(iLeft, iRight), output, (output == NULL));

_exit:
  if (output) {
    taosMemoryFree(output);
  }

  return code;
}

int32_t executeIntersectsFunc(SGeosContext* context, SColumnInfoData *pInputData[],
                              int32_t iLeft, int32_t iRight, SColumnInfoData *pOutputData) {
  int32_t code = TSDB_CODE_FAILED;

  char res = 0;
  code = doIntersectsFunc(context, colDataGetData(pInputData[0], iLeft), colDataGetData(pInputData[1], iRight), &res);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  colDataAppend(pOutputData, TMAX(iLeft, iRight), &res, (res==-1));

  return code;
}

int32_t geomOneParamFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput,
                             _geomPrepareFunc_fn_t prepareFn, _geomExecuteOneParamFunc_fn_t executeOneParamFn) {
  int32_t code = TSDB_CODE_FAILED;

  SColumnInfoData *pInputData = pInput->columnData;
  SColumnInfoData *pOutputData = pOutput->columnData;

  SGeosContext* context = getThreadLocalGeosCtx();
  code = prepareFn(context);
  if (code != TSDB_CODE_SUCCESS) {
    goto _exit;
  }

  for (int32_t i = 0; i < pInput->numOfRows; ++i) {
    if (colDataIsNull_s(pInputData, i)) {
      colDataAppendNULL(pOutputData, i);
      code = TSDB_CODE_SUCCESS;
      continue;
    }

    code = executeOneParamFn(context, pInputData, i, pOutputData);
    if (code != TSDB_CODE_SUCCESS) {
      goto _exit;
    }
  }

  pOutput->numOfRows = pInput->numOfRows;

_exit:
  return code;
}

int32_t geomTwoParamsFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput,
                              _geomPrepareFunc_fn_t prepareFn, _geomExecuteTwoParamsFunc_fn_t executeTwoParamsFn) {
  int32_t code = TSDB_CODE_FAILED;

  SColumnInfoData *pInputData[inputNum];
  SColumnInfoData *pOutputData = pOutput->columnData;
  for (int32_t i = 0; i < inputNum; ++i) {
    pInputData[i] = pInput[i].columnData;
  }

  SGeosContext* context = getThreadLocalGeosCtx();;
  code = prepareFn(context);
  if (code != TSDB_CODE_SUCCESS) {
    goto _exit;
  }

  bool hasNullType = (IS_NULL_TYPE(GET_PARAM_TYPE(&pInput[0])) ||
                      IS_NULL_TYPE(GET_PARAM_TYPE(&pInput[1])));
  bool isConstantLeft = (pInput[0].numOfRows == 1);
  bool isConstantRight = (pInput[1].numOfRows == 1);
  int32_t numOfRows = TMAX(pInput[0].numOfRows, pInput[1].numOfRows);

  if (hasNullType ||                                           // one of operant is NULL type
     (isConstantLeft && colDataIsNull_s(pInputData[0], 0)) ||  // left operand is constant NULL
     (isConstantRight && colDataIsNull_s(pInputData[1], 0))) { // right operand is constant NULL
    colDataAppendNNULL(pOutputData, 0, numOfRows);
    code = TSDB_CODE_SUCCESS;
  } else {
    int32_t iLeft = 0;
    int32_t iRight = 0;
    for (int32_t i = 0; i < numOfRows; ++i) {
      iLeft = isConstantLeft ? 0 : i;
      iRight = isConstantRight ? 0 : i;;

      if ((!isConstantLeft && colDataIsNull_s(pInputData[0], iLeft)) ||
          (!isConstantRight && colDataIsNull_s(pInputData[1], iRight))) {
        colDataAppendNULL(pOutputData, i);
        code = TSDB_CODE_SUCCESS;
        continue;
      }

      code = executeTwoParamsFn(context, pInputData, iLeft, iRight, pOutputData);
      if (code != TSDB_CODE_SUCCESS) {
        goto _exit;
      }
    }
  }

  pOutput->numOfRows = numOfRows;

_exit:
  return code;
}

int32_t geomFromTextFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  return geomOneParamFunction(pInput, inputNum, pOutput, prepareGeomFromText, executeGeomFromTextFunc);
}

int32_t asTextFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  return geomOneParamFunction(pInput, inputNum, pOutput, prepareAsText, executeAsTextFunc);
}

int32_t makePointFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  return geomTwoParamsFunction(pInput, inputNum, pOutput, prepareMakePoint, executeMakePointFunc);
}

int32_t intersectsFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  return geomTwoParamsFunction(pInput, inputNum, pOutput, prepareIntersects, executeIntersectsFunc);
}
