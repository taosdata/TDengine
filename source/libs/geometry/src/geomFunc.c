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
int32_t doGeomFromTextFunc(SGEOSGeomFromTextContext *context, const char *input, unsigned char **output) {
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

int32_t asText(GEOSContextHandle_t handle, GEOSWKBReader *reader, GEOSWKTWriter *writer,
               unsigned char *input, unsigned char *output) {
  int32_t code = TSDB_CODE_FAILED;

  GEOSGeometry *geom = NULL;
  unsigned char *wkt = NULL;

  geom = GEOSWKBReader_read_r(handle, reader, varDataVal(input), varDataLen(input));
  if (geom == NULL) {
    code = TSDB_CODE_FUNC_FUNTION_PARA_VALUE;
    goto _exit;
  }

  wkt = GEOSWKTWriter_write_r(handle, writer, geom);
  if (wkt == NULL) {
    goto _exit;
  }
  size_t size = strlen(wkt);

  memcpy(varDataVal(output), wkt, size);
  varDataSetLen(output, size);
  code = TSDB_CODE_SUCCESS;

_exit:
  if (wkt) {
    GEOSFree_r(handle, wkt);
  }
  if (geom) {
    GEOSGeom_destroy_r(handle, geom);
  }

  return code;
}

int32_t makePoint(GEOSContextHandle_t handle, GEOSWKBWriter *writer,
                  double x, double y, unsigned char *output) {
  int32_t code = TSDB_CODE_FAILED;

  GEOSGeometry *geom = NULL;
  unsigned char *wkb = NULL;

  geom = GEOSGeom_createPointFromXY_r(handle, x, y);
  if (geom == NULL) {
    code = TSDB_CODE_FUNC_FUNTION_PARA_VALUE;
    goto _exit;
  }

  size_t size = 0;
  wkb = GEOSWKBWriter_write_r(handle, writer, geom, &size);
  if (wkb == NULL) {
    goto _exit;
  }

  memcpy(varDataVal(output), wkb, size);
  varDataSetLen(output, size);
  code = TSDB_CODE_SUCCESS;

_exit:
  if (wkb) {
    GEOSFree_r(handle, wkb);
  }
  if (geom) {
    GEOSGeom_destroy_r(handle, geom);
  }

  return code;
}

int32_t geomFromTextFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  int32_t code = TSDB_CODE_FAILED;

  SColumnInfoData *pInputData = pInput->columnData;
  SColumnInfoData *pOutputData = pOutput->columnData;

  SGEOSGeomFromTextContext context = {0};
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

    colDataAppend(pOutputData, i, output, false);

    if (output) {
      taosMemoryFree(output);
      output = NULL;
    }
  }

  pOutput->numOfRows = pInput->numOfRows;

_exit:
  cleanGeomFromText(&context);

  return code;
}

int32_t asTextFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  int32_t code = TSDB_CODE_FAILED;

  SColumnInfoData *pInputData = pInput->columnData;
  SColumnInfoData *pOutputData = pOutput->columnData;

  GEOSContextHandle_t handle = GEOS_init_r();
  GEOSWKBReader *reader = NULL;
  GEOSWKTWriter *writer = NULL;
  unsigned char *output = NULL;

  reader = GEOSWKBReader_create_r(handle);
  if (reader == NULL) {
    goto _exit;
  }
  writer = GEOSWKTWriter_create_r(handle);
  if (writer == NULL) {
    goto _exit;
  }
  output = taosMemoryCalloc(1, TSDB_MAX_BINARY_LEN);
  if (output == NULL) {
    code = TSDB_CODE_TSC_OUT_OF_MEMORY;
    goto _exit;
  }

  for (int32_t i = 0; i < pInput->numOfRows; ++i) {
    if (colDataIsNull_s(pInputData, i)) {
      colDataAppendNULL(pOutputData, i);
      code = TSDB_CODE_SUCCESS;
      continue;
    }

    unsigned char *input = colDataGetData(pInputData, i);
    code = asText(handle, reader, writer, input, output);
    if (code != TSDB_CODE_SUCCESS) {
      goto _exit;
    }

    colDataAppend(pOutputData, i, output, false);
  }

  pOutput->numOfRows = pInput->numOfRows;

_exit:
  if (output) {
    taosMemoryFree(output);
  }
  if (writer) {
    GEOSWKTWriter_destroy_r(handle, writer);
  }
  if (reader) {
    GEOSWKBReader_destroy_r(handle, reader);
  }
  GEOS_finish_r(handle);

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

  GEOSContextHandle_t handle = GEOS_init_r();
  GEOSWKBWriter *writer = NULL;
  unsigned char *output = NULL;

  writer = GEOSWKBWriter_create_r(handle);
  if (writer == NULL) {
    goto _exit;
  }
  output = taosMemoryCalloc(1, TSDB_MAX_GEOMETRY_LEN);
  if (output == NULL) {
    code = TSDB_CODE_TSC_OUT_OF_MEMORY;
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

      code = makePoint(handle, writer, getValueFn[0](pInputData[0]->pData, i), getValueFn[1](pInputData[1]->pData, i), output);
      if (code != TSDB_CODE_SUCCESS) {
        goto _exit;
      }
      colDataAppend(pOutputData, i, output, false);
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

        code = makePoint(handle, writer, getValueFn[0](pInputData[0]->pData, 0), getValueFn[1](pInputData[1]->pData, i), output);
        if (code != TSDB_CODE_SUCCESS) {
          goto _exit;
        }
        colDataAppend(pOutputData, i, output, false);
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

        code = makePoint(handle, writer, getValueFn[0](pInputData[0]->pData, i), getValueFn[1](pInputData[1]->pData, 0), output);
        if (code != TSDB_CODE_SUCCESS) {
          goto _exit;
        }
        colDataAppend(pOutputData, i, output, false);
      }
    }
  }

  pOutput->numOfRows = numOfRows;

_exit:
  if (output) {
    taosMemoryFree(output);
  }
  if (writer) {
    GEOSWKBWriter_destroy_r(handle, writer);
  }
  GEOS_finish_r(handle);

  return code;
}
