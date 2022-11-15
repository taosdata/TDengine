#include <geos_c.h>
#include "geomfunc.h"
#include "querynodes.h"
#include "tdatablock.h"
#include "sclInt.h"
#include "sclvector.h"

static int32_t geomFromText(GEOSContextHandle_t handle, GEOSWKTReader *reader, GEOSWKBWriter *writer, unsigned char *input, unsigned char *output) {
  int32_t code = TSDB_CODE_FAILED;

  GEOSGeometry *geom = 0;
  unsigned char *wkb = 0;

  char *end = varDataVal(input) + varDataLen(input);
  char endValue = *end;
  *end = 0; //make input as a zero ending string
  geom = GEOSWKTReader_read_r(handle, reader, varDataVal(input));
  *end = endValue;  //recover the string after reading it
  if (geom == 0) {
    code = TSDB_CODE_FUNC_FUNTION_PARA_VALUE;
    goto _exit;
  }

  size_t size = 0;
  wkb = GEOSWKBWriter_write_r(handle, writer, geom, &size);
  if (wkb == 0) {
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

static int32_t asText(GEOSContextHandle_t handle, GEOSWKBReader *reader, GEOSWKTWriter *writer, unsigned char *input, unsigned char *output) {
  int32_t code = TSDB_CODE_FAILED;

  GEOSGeometry *geom = 0;
  unsigned char *wkt = 0;

  geom = GEOSWKBReader_read_r(handle, reader, varDataVal(input), varDataLen(input));
  if (geom == 0) {
    code = TSDB_CODE_FUNC_FUNTION_PARA_VALUE;
    goto _exit;
  }

  wkt = GEOSWKTWriter_write_r(handle, writer, geom);
  if (wkt == 0) {
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

static int32_t makePoint(GEOSContextHandle_t handle, GEOSWKBWriter *writer, double x, double y, unsigned char *output) {
  int32_t code = TSDB_CODE_FAILED;

  GEOSGeometry *geom = 0;
  unsigned char *wkb = 0;

  geom = GEOSGeom_createPointFromXY_r(handle, x, y);
  if (geom == 0) {
    code = TSDB_CODE_FUNC_FUNTION_PARA_VALUE;
    goto _exit;
  }

  size_t size = 0;
  wkb = GEOSWKBWriter_write_r(handle, writer, geom, &size);
  if (wkb == 0) {
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

  GEOSContextHandle_t handle = GEOS_init_r();
  GEOSWKTReader *reader = 0;
  GEOSWKBWriter *writer = 0;
  char *output = 0;

  reader = GEOSWKTReader_create_r(handle);
  if (reader == 0) {
    goto _exit;
  }
  writer = GEOSWKBWriter_create_r(handle);
  if (writer == 0) {
    goto _exit;
  }
  output = taosMemoryCalloc(TSDB_MAX_GEOMETRY_LEN, 1);
  if (output == 0) {
    goto _exit;
  }

  for (int32_t i = 0; i < pInput->numOfRows; ++i) {
    if (colDataIsNull_s(pInputData, i)) {
      colDataAppendNULL(pOutputData, i);
      continue;
    }

    char *input = colDataGetData(pInputData, i);
    code = geomFromText(handle, reader, writer, input, output);
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
    GEOSWKBWriter_destroy_r(handle, writer);
  }
  if (reader) {
    GEOSWKTReader_destroy_r(handle, reader);
  }
  GEOS_finish_r(handle);

  return code;
}

int32_t asTextFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  int32_t code = TSDB_CODE_FAILED;

  SColumnInfoData *pInputData = pInput->columnData;
  SColumnInfoData *pOutputData = pOutput->columnData;

  GEOSContextHandle_t handle = GEOS_init_r();
  GEOSWKBReader *reader = 0;
  GEOSWKTWriter *writer = 0;
  char *output = 0;

  reader = GEOSWKBReader_create_r(handle);
  if (reader == 0) {
    goto _exit;
  }
  writer = GEOSWKTWriter_create_r(handle);
  if (writer == 0) {
    goto _exit;
  }
  output = taosMemoryCalloc(TSDB_MAX_BINARY_LEN, 1);
  if (output == 0) {
    goto _exit;
  }

  for (int32_t i = 0; i < pInput->numOfRows; ++i) {
    if (colDataIsNull_s(pInputData, i)) {
      colDataAppendNULL(pOutputData, i);
      continue;
    }

    char *input = colDataGetData(pInputData, i);
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
  GEOSWKBWriter *writer = 0;
  char *output = 0;

  writer = GEOSWKBWriter_create_r(handle);
  if (writer == 0) {
    goto _exit;
  }
  output = taosMemoryCalloc(TSDB_MAX_GEOMETRY_LEN, 1);
  if (output == 0) {
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
    } else {
      for (int32_t i = 0; i < numOfRows; ++i) {
        if (colDataIsNull_s(pInputData[1], i)) {
          colDataAppendNULL(pOutputData, i);
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
    } else {
      for (int32_t i = 0; i < numOfRows; ++i) {
        if (colDataIsNull_s(pInputData[0], i)) {
          colDataAppendNULL(pOutputData, i);
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
