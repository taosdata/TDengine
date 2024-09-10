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

typedef int32_t (*_geomDoRelationFunc_t)(const GEOSGeometry *geom1, const GEOSPreparedGeometry *preparedGeom1,
                                         const GEOSGeometry *geom2, bool swapped, char *res);

typedef int32_t (*_geomInitCtxFunc_t)();
typedef int32_t (*_geomExecuteOneParamFunc_t)(SColumnInfoData *pInputData, int32_t i, SColumnInfoData *pOutputData);
typedef int32_t (*_geomExecuteTwoParamsFunc_t)(SColumnInfoData *pInputData[], int32_t iLeft, int32_t iRight,
                                               SColumnInfoData *pOutputData);

// output is with VARSTR format
// need to call taosMemoryFree(*output) later
int32_t doMakePointFunc(double x, double y, unsigned char **output) {
  int32_t code = TSDB_CODE_FAILED;

  unsigned char *outputGeom = NULL;
  size_t         size = 0;
  code = doMakePoint(x, y, &outputGeom, &size);
  if (code != TSDB_CODE_SUCCESS) {
    goto _exit;
  }

  *output = taosMemoryCalloc(1, size + VARSTR_HEADER_SIZE);
  if (*output == NULL) {
    code = terrno;
    goto _exit;
  }

  (void)memcpy(varDataVal(*output), outputGeom, size);
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

  if ((varDataLen(input)) == 0) {  // empty value
    *output = NULL;
    return TSDB_CODE_SUCCESS;
  }

  char          *inputGeom = NULL;
  unsigned char *outputGeom = NULL;
  size_t         size = 0;

  // make a zero ending string
  inputGeom = taosMemoryCalloc(1, varDataLen(input) + 1);
  if (inputGeom == NULL) {
    code = terrno;
    goto _exit;
  }
  (void)memcpy(inputGeom, varDataVal(input), varDataLen(input));

  TAOS_CHECK_GOTO(doGeomFromText(inputGeom, &outputGeom, &size), NULL, _exit);

  *output = taosMemoryCalloc(1, size + VARSTR_HEADER_SIZE);
  if (*output == NULL) {
    code = terrno;
    goto _exit;
  }

  (void)memcpy(varDataVal(*output), outputGeom, size);
  varDataSetLen(*output, size);
  code = TSDB_CODE_SUCCESS;

_exit:
  geosFreeBuffer(outputGeom);
  geosFreeBuffer(inputGeom);

  return code;
}

// both input and output are with VARSTR format
// need to call taosMemoryFree(*output) later
int32_t doAsTextFunc(unsigned char *input, char **output) {
  int32_t code = TSDB_CODE_FAILED;

  if ((varDataLen(input)) == 0) {  // empty value
    *output = NULL;
    return TSDB_CODE_SUCCESS;
  }

  char *outputWKT = NULL;
  TAOS_CHECK_GOTO(doAsText(varDataVal(input), varDataLen(input), &outputWKT), NULL, _exit);

  size_t size = strlen(outputWKT);
  *output = taosMemoryCalloc(1, size + VARSTR_HEADER_SIZE);
  if (*output == NULL) {
    code = terrno;
    goto _exit;
  }

  (void)memcpy(varDataVal(*output), outputWKT, size);
  varDataSetLen(*output, size);
  code = TSDB_CODE_SUCCESS;

_exit:
  geosFreeBuffer(outputWKT);

  return code;
}

int32_t executeMakePointFunc(SColumnInfoData *pInputData[], int32_t iLeft, int32_t iRight,
                             SColumnInfoData *pOutputData) {
  int32_t code = TSDB_CODE_FAILED;
  unsigned char *output = NULL;

  _getDoubleValue_fn_t getDoubleValueFn[2];
  TAOS_CHECK_GOTO(getVectorDoubleValueFn(pInputData[0]->info.type, &getDoubleValueFn[0]), NULL, _exit);
  TAOS_CHECK_GOTO(getVectorDoubleValueFn(pInputData[1]->info.type, &getDoubleValueFn[1]), NULL, _exit);

  double         leftRes = 0;
  double         rightRes = 0;

  TAOS_CHECK_GOTO(getDoubleValueFn[0](pInputData[0]->pData, iLeft, &leftRes), NULL, _exit);
  TAOS_CHECK_GOTO(getDoubleValueFn[1](pInputData[1]->pData, iRight, &rightRes), NULL, _exit);
  TAOS_CHECK_GOTO(doMakePointFunc(leftRes, rightRes, &output), NULL, _exit);
  TAOS_CHECK_GOTO(colDataSetVal(pOutputData, TMAX(iLeft, iRight), output, (output == NULL)), NULL, _exit);

_exit:
  if (output) {
    taosMemoryFree(output);
  }

  return code;
}

int32_t executeGeomFromTextFunc(SColumnInfoData *pInputData, int32_t i, SColumnInfoData *pOutputData) {
  int32_t code = TSDB_CODE_FAILED;

  if (!IS_VAR_DATA_TYPE((pInputData)->info.type)) {
    return TSDB_CODE_FUNC_FUNTION_PARA_VALUE;
  }

  char          *input = colDataGetData(pInputData, i);
  unsigned char *output = NULL;

  TAOS_CHECK_GOTO(doGeomFromTextFunc(input, &output), NULL, _exit);
  TAOS_CHECK_GOTO(colDataSetVal(pOutputData, i, output, (output == NULL)), NULL, _exit);

_exit:
  if (output) {
    taosMemoryFree(output);
  }

  return code;
}

int32_t executeAsTextFunc(SColumnInfoData *pInputData, int32_t i, SColumnInfoData *pOutputData) {
  int32_t code = TSDB_CODE_FAILED;

  unsigned char *input = colDataGetData(pInputData, i);
  char          *output = NULL;

  TAOS_CHECK_GOTO(doAsTextFunc(input, &output), NULL, _exit);
  TAOS_CHECK_GOTO(colDataSetVal(pOutputData, i, output, (output == NULL)), NULL, _exit);

_exit:
  if (output) {
    taosMemoryFree(output);
  }

  return code;
}

int32_t executeRelationFunc(const GEOSGeometry *geom1, const GEOSPreparedGeometry *preparedGeom1,
                            const GEOSGeometry *geom2, int32_t i, bool swapped, SColumnInfoData *pOutputData,
                            _geomDoRelationFunc_t doRelationFn) {
  char res = 0;

  if (!geom1 || !geom2) {  // if empty input value
    res = -1;
  } else {
    TAOS_CHECK_RETURN(doRelationFn(geom1, preparedGeom1, geom2, swapped, &res));
  }

  return colDataSetVal(pOutputData, i, &res, (res == -1));
}

int32_t geomOneParamFunction(SScalarParam *pInput, SScalarParam *pOutput, _geomInitCtxFunc_t initCtxFn,
                             _geomExecuteOneParamFunc_t executeOneParamFn) {
  TAOS_CHECK_RETURN(initCtxFn());

  SColumnInfoData *pInputData = pInput->columnData;
  SColumnInfoData *pOutputData = pOutput->columnData;
  pOutput->numOfRows = pInput->numOfRows;

  if (IS_NULL_TYPE(GET_PARAM_TYPE(pInput))) {
    colDataSetNNULL(pOutputData, 0, pInput->numOfRows);
  } else {
    for (int32_t i = 0; i < pInput->numOfRows; ++i) {
      if (colDataIsNull_s(pInputData, i)) {
        colDataSetNULL(pOutputData, i);
        continue;
      }

      TAOS_CHECK_RETURN(executeOneParamFn(pInputData, i, pOutputData));
    }
  }

  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

int32_t geomTwoParamsFunction(SScalarParam *pInput, SScalarParam *pOutput, _geomInitCtxFunc_t initCtxFn,
                              _geomExecuteTwoParamsFunc_t executeTwoParamsFn) {
  TAOS_CHECK_RETURN(initCtxFn());

  SColumnInfoData *pInputData[2];
  SColumnInfoData *pOutputData = pOutput->columnData;
  pInputData[0] = pInput[0].columnData;
  pInputData[1] = pInput[1].columnData;

  bool    hasNullType = (IS_NULL_TYPE(GET_PARAM_TYPE(&pInput[0])) || IS_NULL_TYPE(GET_PARAM_TYPE(&pInput[1])));
  bool    isConstantLeft = (pInput[0].numOfRows == 1);
  bool    isConstantRight = (pInput[1].numOfRows == 1);
  int32_t numOfRows = TMAX(pInput[0].numOfRows, pInput[1].numOfRows);
  pOutput->numOfRows = numOfRows;

  if (hasNullType ||                                             // one of operant is NULL type
      (isConstantLeft && colDataIsNull_s(pInputData[0], 0)) ||   // left operand is constant NULL
      (isConstantRight && colDataIsNull_s(pInputData[1], 0))) {  // right operand is constant NULL
    colDataSetNNULL(pOutputData, 0, numOfRows);
  } else {
    int32_t iLeft = 0;
    int32_t iRight = 0;
    for (int32_t i = 0; i < numOfRows; ++i) {
      iLeft = isConstantLeft ? 0 : i;
      iRight = isConstantRight ? 0 : i;

      if ((!isConstantLeft && colDataIsNull_s(pInputData[0], iLeft)) ||
          (!isConstantRight && colDataIsNull_s(pInputData[1], iRight))) {
        colDataSetNULL(pOutputData, i);
        continue;
      }

      TAOS_CHECK_RETURN(executeTwoParamsFn(pInputData, iLeft, iRight, pOutputData));
    }
  }

  TAOS_RETURN(TSDB_CODE_SUCCESS);
}

int32_t geomRelationFunction(SScalarParam *pInput, SScalarParam *pOutput, bool swapAllowed,
                             _geomDoRelationFunc_t doRelationFn) {
  int32_t code = TSDB_CODE_FAILED;

  TAOS_CHECK_RETURN(initCtxRelationFunc());

  // handle with all NULL output
  bool    hasNullType = (IS_NULL_TYPE(GET_PARAM_TYPE(&pInput[0])) || IS_NULL_TYPE(GET_PARAM_TYPE(&pInput[1])));
  bool    isConstant1 = (pInput[0].numOfRows == 1);
  bool    isConstant2 = (pInput[1].numOfRows == 1);
  int32_t numOfRows = TMAX(pInput[0].numOfRows, pInput[1].numOfRows);
  pOutput->numOfRows = numOfRows;
  SColumnInfoData *pOutputData = pOutput->columnData;

  if (hasNullType ||                                                // at least one of operant is NULL type
      (isConstant1 && colDataIsNull_s(pInput[0].columnData, 0)) ||  // left operand is constant NULL
      (isConstant2 && colDataIsNull_s(pInput[1].columnData, 0))) {  // right operand is constant NULL
    colDataSetNNULL(pOutputData, 0, numOfRows);
    TAOS_RETURN(TSDB_CODE_SUCCESS);
  }

  bool             swapped = false;
  SColumnInfoData *pInputData[2];

  // swap two input data to make sure input data 0 is constant if swapAllowed and only isConstant2 is true
  if (swapAllowed && !isConstant1 && isConstant2) {
    pInputData[0] = pInput[1].columnData;
    pInputData[1] = pInput[0].columnData;

    isConstant1 = true;
    isConstant2 = false;
    swapped = true;
  } else {
    pInputData[0] = pInput[0].columnData;
    pInputData[1] = pInput[1].columnData;
  }

  GEOSGeometry               *geom1 = NULL;
  GEOSGeometry               *geom2 = NULL;
  const GEOSPreparedGeometry *preparedGeom1 = NULL;

  // if there is constant, make PreparedGeometry from pInputData 0
  if (isConstant1) {
    TAOS_CHECK_GOTO(readGeometry(colDataGetData(pInputData[0], 0), &geom1, &preparedGeom1), NULL, _exit);
  }
  if (isConstant2) {
    TAOS_CHECK_GOTO(readGeometry(colDataGetData(pInputData[1], 0), &geom2, NULL), NULL, _exit);
  }

  for (int32_t i = 0; i < numOfRows; ++i) {
    if ((!isConstant1 && colDataIsNull_s(pInputData[0], i)) || (!isConstant2 && colDataIsNull_s(pInputData[1], i))) {
      colDataSetNULL(pOutputData, i);
      continue;
    }

    if (!isConstant1) {
      TAOS_CHECK_GOTO(readGeometry(colDataGetData(pInputData[0], i), &geom1, &preparedGeom1), NULL, _exit);
    }
    if (!isConstant2) {
      TAOS_CHECK_GOTO(readGeometry(colDataGetData(pInputData[1], i), &geom2, NULL), NULL, _exit);
    }

    TAOS_CHECK_GOTO(executeRelationFunc(geom1, preparedGeom1, geom2, i, swapped, pOutputData, doRelationFn), NULL,
                    _exit);

    if (!isConstant1) {
      destroyGeometry(&geom1, &preparedGeom1);
    }
    if (!isConstant2) {
      destroyGeometry(&geom2, NULL);
    }
  }

  code = TSDB_CODE_SUCCESS;

_exit:
  destroyGeometry(&geom1, &preparedGeom1);
  destroyGeometry(&geom2, NULL);

  TAOS_RETURN(code);
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
  return geomRelationFunction(pInput, pOutput, true, doIntersects);
}

int32_t equalsFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  return geomRelationFunction(pInput, pOutput, true, doEquals);
}

int32_t touchesFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  return geomRelationFunction(pInput, pOutput, true, doTouches);
}

int32_t coversFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  return geomRelationFunction(pInput, pOutput, true, doCovers);
}

int32_t containsFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  return geomRelationFunction(pInput, pOutput, true, doContains);
}

int32_t containsProperlyFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  return geomRelationFunction(pInput, pOutput, false, doContainsProperly);
}
