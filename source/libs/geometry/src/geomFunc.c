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

typedef int32_t (*_geomInitCtxFunc_t)();
typedef int32_t (*_geomExecuteOneParamFunc_t)(SColumnInfoData *pInputData, int32_t i, SColumnInfoData *pOutputData);
typedef int32_t (*_geomExecuteTwoParamsFunc_t)(SColumnInfoData *pInputData[], int32_t iLeft, int32_t iRight,
                                               SColumnInfoData *pOutputData);
typedef int32_t (*_geomExecutePreparedFunc_t)(const GEOSPreparedGeometry *preparedGeom1, SColumnInfoData *pInputData2, int32_t i2,
                                              SColumnInfoData *pOutputData);

// output is with VARSTR format
// need to call taosMemoryFree(*output) later
int32_t doMakePointFunc(double x, double y, unsigned char **output) {
  int32_t code = TSDB_CODE_FAILED;

  unsigned char *outputGeom = NULL;
  size_t size = 0;
  code = doMakePoint(x, y, &outputGeom, &size);
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
  geosFreeBuffer(outputGeom);

  return code;
}

// both input and output are with VARSTR format
// need to call taosMemoryFree(*output) later
int32_t doGeomFromTextFunc(const char *input, unsigned char **output) {
  int32_t code = TSDB_CODE_FAILED;

  if ((varDataLen(input)) == 0) { //empty value
    *output = NULL;
    return TSDB_CODE_SUCCESS;
  }

  // make input as a zero ending string
  char *end = varDataVal(input) + varDataLen(input);
  char endValue = *end;
  *end = 0; 

  unsigned char *outputGeom = NULL;
  size_t size = 0;

  code = doGeomFromText(varDataVal(input), &outputGeom, &size);
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
  geosFreeBuffer(outputGeom);

  *end = endValue;  //recover the input string

  return code;
}

// both input and output are with VARSTR format
// need to call taosMemoryFree(*output) later
int32_t doAsTextFunc(unsigned char *input, char **output) {
  int32_t code = TSDB_CODE_FAILED;

  if ((varDataLen(input)) == 0) { //empty value
    *output = NULL;
    return TSDB_CODE_SUCCESS;
  }

  char *outputWKT = NULL;
  code = doAsText(varDataVal(input), varDataLen(input), &outputWKT);
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
  geosFreeBuffer(outputWKT);

  return code;
}

// both input1 and input2 are with VARSTR format
int32_t doIntersectsFunc(unsigned char *input1, unsigned char *input2, char *res) {
  int32_t code = TSDB_CODE_FAILED;

  if (varDataLen(input1) == 0 || varDataLen(input2) == 0) { //empty value
    *res = -1;  // it means a NULL result
    return TSDB_CODE_SUCCESS;
  }

  code = doIntersects(varDataVal(input1), varDataLen(input1),
                      varDataVal(input2), varDataLen(input2),
                      res);

  return code;
}

int32_t executeMakePointFunc(SColumnInfoData *pInputData[], int32_t iLeft, int32_t iRight,
                             SColumnInfoData *pOutputData) {
  int32_t code = TSDB_CODE_FAILED;

  _getDoubleValue_fn_t getDoubleValueFn[2];
  getDoubleValueFn[0]= getVectorDoubleValueFn(pInputData[0]->info.type);
  getDoubleValueFn[1]= getVectorDoubleValueFn(pInputData[1]->info.type);

  unsigned char *output = NULL;
  code = doMakePointFunc(getDoubleValueFn[0](pInputData[0]->pData, iLeft), getDoubleValueFn[1](pInputData[1]->pData, iRight), &output);
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

int32_t executeGeomFromTextFunc(SColumnInfoData *pInputData, int32_t i, SColumnInfoData *pOutputData) {
  int32_t code = TSDB_CODE_FAILED;

  char *input = colDataGetData(pInputData, i);
  unsigned char *output = NULL;
  code = doGeomFromTextFunc(input, &output);
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

int32_t executeAsTextFunc(SColumnInfoData *pInputData, int32_t i, SColumnInfoData *pOutputData) {
  int32_t code = TSDB_CODE_FAILED;

  unsigned char *input = colDataGetData(pInputData, i);
  char *output = NULL;
  code = doAsTextFunc(input, &output);
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

int32_t executeIntersectsFunc(SColumnInfoData *pInputData[], int32_t i1, int32_t i2,
                              SColumnInfoData *pOutputData) {
  int32_t code = TSDB_CODE_FAILED;

  char res = 0;
  code = doIntersectsFunc(colDataGetData(pInputData[0], i1), colDataGetData(pInputData[1], i2), &res);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  colDataAppend(pOutputData, TMAX(i1, i2), &res, (res==-1));

  return code;
}

int32_t executePreparedIntersectsFunc(const GEOSPreparedGeometry *preparedGeom1, SColumnInfoData *pInputData2, int32_t i2,
                                      SColumnInfoData *pOutputData) {
  int32_t code = TSDB_CODE_FAILED;

  char res = 0;
  unsigned char *input2 = colDataGetData(pInputData2, i2);

  code = doPreparedIntersects(preparedGeom1, varDataVal(input2), varDataLen(input2), &res);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  colDataAppend(pOutputData, i2, &res, (res==-1));

  return code;
}

int32_t geomOneParamFunction(SScalarParam *pInput, SScalarParam *pOutput,
                             _geomInitCtxFunc_t initCtxFn, _geomExecuteOneParamFunc_t executeOneParamFn) {
  int32_t code = TSDB_CODE_FAILED;

  SColumnInfoData *pInputData = pInput->columnData;
  SColumnInfoData *pOutputData = pOutput->columnData;

  code = initCtxFn();
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  if (IS_NULL_TYPE(GET_PARAM_TYPE(pInput))) {
    colDataAppendNNULL(pOutputData, 0, pInput->numOfRows);
    code = TSDB_CODE_SUCCESS;
  }
  else {
    for (int32_t i = 0; i < pInput->numOfRows; ++i) {
      if (colDataIsNull_s(pInputData, i)) {
        colDataAppendNULL(pOutputData, i);
        code = TSDB_CODE_SUCCESS;
        continue;
      }

      code = executeOneParamFn(pInputData, i, pOutputData);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    }
  }

  pOutput->numOfRows = pInput->numOfRows;

  return code;
}

int32_t geomTwoParamsFunction(SScalarParam *pInput, SScalarParam *pOutput,
                              _geomInitCtxFunc_t initCtxFn, _geomExecuteTwoParamsFunc_t executeTwoParamsFn) {
  int32_t code = TSDB_CODE_FAILED;
  int32_t inputNum = 2;

  SColumnInfoData *pInputData[inputNum];
  SColumnInfoData *pOutputData = pOutput->columnData;
  for (int32_t i = 0; i < inputNum; ++i) {
    pInputData[i] = pInput[i].columnData;
  }

  code = initCtxFn();
  if (code != TSDB_CODE_SUCCESS) {
    return code;
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
  }
  else {
    int32_t iLeft = 0;
    int32_t iRight = 0;
    for (int32_t i = 0; i < numOfRows; ++i) {
      iLeft = isConstantLeft ? 0 : i;
      iRight = isConstantRight ? 0 : i;

      if ((!isConstantLeft && colDataIsNull_s(pInputData[0], iLeft)) ||
          (!isConstantRight && colDataIsNull_s(pInputData[1], iRight))) {
        colDataAppendNULL(pOutputData, i);
        code = TSDB_CODE_SUCCESS;
        continue;
      }

      code = executeTwoParamsFn(pInputData, iLeft, iRight, pOutputData);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    }
  }

  pOutput->numOfRows = numOfRows;

  return code;
}

int32_t geomPreparedSwappableFunction(SScalarParam *pInput, SScalarParam *pOutput,
                                      _geomInitCtxFunc_t initCtxFn,
                                      _geomExecuteTwoParamsFunc_t executeTwoParamsFn,
                                      _geomExecutePreparedFunc_t executePreparedFn) {
  int32_t code = TSDB_CODE_FAILED;
  int32_t inputNum = 2;

  SColumnInfoData *pInputData[inputNum];
  SColumnInfoData *pOutputData = pOutput->columnData;
  for (int32_t i = 0; i < inputNum; ++i) {
    pInputData[i] = pInput[i].columnData;
  }

  code = initCtxFn();
  if (code != TSDB_CODE_SUCCESS) {
    goto _exit;
  }

  bool hasNullType = (IS_NULL_TYPE(GET_PARAM_TYPE(&pInput[0])) ||
                      IS_NULL_TYPE(GET_PARAM_TYPE(&pInput[1])));
  bool isConstant1 = (pInput[0].numOfRows == 1);
  bool isConstant2 = (pInput[1].numOfRows == 1);
  int32_t numOfRows = TMAX(pInput[0].numOfRows, pInput[1].numOfRows);

  GEOSGeometry *geom1 = NULL;
  const GEOSPreparedGeometry *preparedGeom1 = NULL;
  SColumnInfoData *pInputData2 = NULL;

  if (hasNullType ||                                       // one of operant is NULL type
     (isConstant1 && colDataIsNull_s(pInputData[0], 0)) || // left operand is constant NULL
     (isConstant2 && colDataIsNull_s(pInputData[1], 0))) { // right operand is constant NULL
    colDataAppendNNULL(pOutputData, 0, numOfRows);
    code = TSDB_CODE_SUCCESS;
  }
  else {
    // only if two params are from 1 to X, make PreparedGeometry for the one param and call executePreparedFn()
    int32_t constantIndex = -1;
    int32_t scalarIndex = -1;
    if (isConstant1 && !isConstant2) { // make first param constant to be PreparedGeometry as input1
      constantIndex = 0;
      scalarIndex = 1;
    }
    else if (isConstant2 && !isConstant1) { // make second param constant to be PreparedGeometry as input1 since the two params are swappable
      constantIndex = 1;
      scalarIndex = 0;
    }

    if (constantIndex != -1) {
      code = makePreparedGeometry(colDataGetData(pInputData[constantIndex], 0), &geom1, &preparedGeom1);
      if (code != TSDB_CODE_SUCCESS) {
        goto _exit;
      }
      pInputData2 = pInputData[scalarIndex];
    }

    int32_t i1 = 0;
    int32_t i2 = 0;
    for (int32_t i = 0; i < numOfRows; ++i) {
      i1 = isConstant1 ? 0 : i;
      i2 = isConstant2 ? 0 : i;;

      if ((!isConstant1 && colDataIsNull_s(pInputData[0], i1)) ||
          (!isConstant2 && colDataIsNull_s(pInputData[1], i2))) {
        colDataAppendNULL(pOutputData, i);
        code = TSDB_CODE_SUCCESS;
        continue;
      }

      if (preparedGeom1) {
        code = executePreparedFn(preparedGeom1, pInputData2, i, pOutputData);
      }
      else {
        code = executeTwoParamsFn(pInputData, i1, i2, pOutputData);
      }
      if (code != TSDB_CODE_SUCCESS) {
        goto _exit;
      }
    }
  }

  pOutput->numOfRows = numOfRows;

_exit:
  destroyPreparedGeometry(&preparedGeom1, &geom1);

  return code;
}

int32_t makePointFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  return geomTwoParamsFunction(pInput, pOutput, initCtxMakePoint, executeMakePointFunc);
}

int32_t geomFromTextFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  return geomOneParamFunction(pInput, pOutput, initCtxGeomFromText, executeGeomFromTextFunc);
}

int32_t asTextFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  return geomOneParamFunction(pInput, pOutput, initCtxAsText, executeAsTextFunc);
}

int32_t intersectsFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  return geomPreparedSwappableFunction(pInput, pOutput,
                                       initCtxIntersects, executeIntersectsFunc, executePreparedIntersectsFunc);
}
