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

// both input and output are with VARSTR format
// need to call taosMemoryFree(*output) later
int32_t doGeomFromTextFunc(SGeosContext *context, const char *input, unsigned char **output) {
  int32_t code = TSDB_CODE_FAILED;

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

void appendOutputData(SColumnInfoData *pOutputData, int32_t i, unsigned char *output) {
  colDataAppend(pOutputData, i, output, false);

  if (output) {
    taosMemoryFree(output);
  }
}

int32_t geomFromTextFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  int32_t code = TSDB_CODE_FAILED;

  SColumnInfoData *pInputData = pInput->columnData;
  SColumnInfoData *pOutputData = pOutput->columnData;

  SGeosContext context = {0};
  code = prepareGeomFromText(&context);
  if (code != TSDB_CODE_SUCCESS) {
    goto _exit;
  }

  for (int32_t i = 0; i < pInput->numOfRows; ++i) {
    if (colDataIsNull_s(pInputData, i)) {
      colDataAppendNULL(pOutputData, i);
      code = TSDB_CODE_SUCCESS;
      continue;
    }

    char *input = colDataGetData(pInputData, i);
    unsigned char *output = NULL;
    code = doGeomFromTextFunc(&context, input, &output);
    if (code != TSDB_CODE_SUCCESS) {
      goto _exit;
    }
    appendOutputData(pOutputData, i, output);
  }

  pOutput->numOfRows = pInput->numOfRows;

_exit:
  destroyGeosContext(&context);

  return code;
}

int32_t asTextFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  int32_t code = TSDB_CODE_FAILED;

  SColumnInfoData *pInputData = pInput->columnData;
  SColumnInfoData *pOutputData = pOutput->columnData;

  SGeosContext context = {0};
  code = prepareAsText(&context);
  if (code != TSDB_CODE_SUCCESS) {
    goto _exit;
  }

  for (int32_t i = 0; i < pInput->numOfRows; ++i) {
    if (colDataIsNull_s(pInputData, i)) {
      colDataAppendNULL(pOutputData, i);
      code = TSDB_CODE_SUCCESS;
      continue;
    }

    unsigned char *input = colDataGetData(pInputData, i);
    char *output = NULL;
    code = doAsTextFunc(&context, input, &output);
    if (code != TSDB_CODE_SUCCESS) {
      goto _exit;
    }
    appendOutputData(pOutputData, i, output);
  }

  pOutput->numOfRows = pInput->numOfRows;

_exit:
  destroyGeosContext(&context);

  return code;
}

int32_t makePointFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  int32_t code = TSDB_CODE_FAILED;

  SColumnInfoData *pInputData[inputNum];
  SColumnInfoData *pOutputData = pOutput->columnData;
  _getDoubleValue_fn_t getValueFn[inputNum];

  for (int32_t i = 0; i < inputNum; ++i) {
    pInputData[i] = pInput[i].columnData;
    getValueFn[i]= getVectorDoubleValueFn(GET_PARAM_TYPE(&pInput[i]));
  }

  SGeosContext context = {0};
  code = prepareMakePoint(&context);
  if (code != TSDB_CODE_SUCCESS) {
    goto _exit;
  }

  bool hasNullType = (IS_NULL_TYPE(GET_PARAM_TYPE(&pInput[0])) ||
                      IS_NULL_TYPE(GET_PARAM_TYPE(&pInput[1])));

  int32_t numOfRows = TMAX(pInput[0].numOfRows, pInput[1].numOfRows);
  if (pInput[0].numOfRows == pInput[1].numOfRows) {
    for (int32_t i = 0; i < numOfRows; ++i) {
      if (colDataIsNull_s(pInputData[0], i) ||
          colDataIsNull_s(pInputData[1], i) ||
          hasNullType) {
        colDataAppendNULL(pOutputData, i);
        code = TSDB_CODE_SUCCESS;
        continue;
      }

      unsigned char *output = NULL;
      code = doMakePointFunc(&context, getValueFn[0](pInputData[0]->pData, i), getValueFn[1](pInputData[1]->pData, i), &output);
      if (code != TSDB_CODE_SUCCESS) {
        goto _exit;
      }
      appendOutputData(pOutputData, i, output);
    }
  } else if (pInput[0].numOfRows == 1) { //left operand is constant
    if (colDataIsNull_s(pInputData[0], 0) || hasNullType) {
      colDataAppendNNULL(pOutputData, 0, pInput[1].numOfRows);
      code = TSDB_CODE_SUCCESS;
    } else {
      for (int32_t i = 0; i < numOfRows; ++i) {
        if (colDataIsNull_s(pInputData[1], i)) {
          colDataAppendNULL(pOutputData, i);
          code = TSDB_CODE_SUCCESS;
          continue;
        }

        unsigned char *output = NULL;
        code = doMakePointFunc(&context, getValueFn[0](pInputData[0]->pData, 0), getValueFn[1](pInputData[1]->pData, i), &output);
        if (code != TSDB_CODE_SUCCESS) {
          goto _exit;
        }
        appendOutputData(pOutputData, i, output);
      }
    }
  } else if (pInput[1].numOfRows == 1) {
    if (colDataIsNull_s(pInputData[1], 0) || hasNullType) {
      colDataAppendNNULL(pOutputData, 0, pInput[0].numOfRows);
      code = TSDB_CODE_SUCCESS;
    } else {
      for (int32_t i = 0; i < numOfRows; ++i) {
        if (colDataIsNull_s(pInputData[0], i)) {
          colDataAppendNULL(pOutputData, i);
          code = TSDB_CODE_SUCCESS;
          continue;
        }

        unsigned char *output = NULL;
        code = doMakePointFunc(&context, getValueFn[0](pInputData[0]->pData, i), getValueFn[1](pInputData[1]->pData, 0), &output);
        if (code != TSDB_CODE_SUCCESS) {
          goto _exit;
        }
        appendOutputData(pOutputData, i, output);
      }
    }
  }

  pOutput->numOfRows = numOfRows;

_exit:
  destroyGeosContext(&context);

  return code;
}
