#include <geos_c.h>
#include "geomfunc.h"
#include "querynodes.h"
#include "tdatablock.h"
#include "sclInt.h"
#include "sclvector.h"

static int32_t geomFromText(unsigned char *input, unsigned char *output) {
  int32_t code = TSDB_CODE_FAILED;

  initGEOS(0, 0);
  GEOSGeometry *geom = 0;
  GEOSWKTReader *reader = 0;
  GEOSWKBWriter *writer = 0;
  unsigned char *wkb = 0;

  reader = GEOSWKTReader_create();
  if (reader == 0) {
    goto _exit;
  }

  char *end = varDataVal(input) + varDataLen(input);
  char endValue = *end;
  *end = 0;
  geom = GEOSWKTReader_read(reader, varDataVal(input));
  *end = endValue;
  if (geom == 0) {
    code = TSDB_CODE_FUNC_FUNTION_PARA_VALUE;
    goto _exit;
  }

  writer = GEOSWKBWriter_create();
  if (writer == 0) {
    goto _exit;
  }

  size_t size = 0;
  wkb = GEOSWKBWriter_write(writer, geom, &size);
  if (wkb == 0) {
    goto _exit;
  }

  memcpy(varDataVal(output), wkb, size);
  varDataSetLen(output, size);
  code = TSDB_CODE_SUCCESS;

_exit:
  if (wkb) {
    GEOSFree(wkb);
  }
  if (writer) {
    GEOSWKBWriter_destroy(writer);
  }
  if (reader) {
    GEOSWKTReader_destroy(reader);
  }
  if (geom) {
    GEOSGeom_destroy(geom);
  }
  finishGEOS();

  return code;
}

static int32_t asText(unsigned char *input, unsigned char *output) {
  int32_t code = TSDB_CODE_FAILED;

  initGEOS(0, 0);
  GEOSGeometry *geom = 0;
  GEOSWKBReader *reader = 0;
  GEOSWKTWriter *writer = 0;
  unsigned char *wkt = 0;

  reader = GEOSWKBReader_create();
  if (reader == 0) {
    goto _exit;
  }

  geom = GEOSWKBReader_read(reader, varDataVal(input), varDataLen(input));
  if (geom == 0) {
    code = TSDB_CODE_FUNC_FUNTION_PARA_VALUE;
    goto _exit;
  }

  writer = GEOSWKTWriter_create();
  if (writer == 0) {
    goto _exit;
  }

  wkt = GEOSWKTWriter_write(writer, geom);
  if (wkt == 0) {
    goto _exit;
  }
  size_t size = strlen(wkt);

  memcpy(varDataVal(output), wkt, size);
  varDataSetLen(output, size);
  code = TSDB_CODE_SUCCESS;

_exit:
  if (wkt) {
    GEOSFree(wkt);
  }
  if (writer) {
    GEOSWKTWriter_destroy(writer);
  }
  if (reader) {
    GEOSWKBReader_destroy(reader);
  }
  if (geom) {
    GEOSGeom_destroy(geom);
  }
  finishGEOS();

  return code;
}

static int32_t makePoint(double x, double y, unsigned char *output) {
  int32_t code = TSDB_CODE_FAILED;

  initGEOS(0, 0);
  GEOSGeometry *geom = 0;
  GEOSWKBWriter *writer = 0;
  unsigned char *wkb = 0;

  geom = GEOSGeom_createPointFromXY(x, y);
  if (geom == 0) {
    code = TSDB_CODE_FUNC_FUNTION_PARA_VALUE;
    goto _exit;
  }

  writer = GEOSWKBWriter_create();
  if (writer == 0) {
    goto _exit;
  }

  size_t size = 0;
  wkb = GEOSWKBWriter_write(writer, geom, &size);
  if (wkb == 0) {
    goto _exit;
  }

  memcpy(varDataVal(output), wkb, size);
  varDataSetLen(output, size);
  code = TSDB_CODE_SUCCESS;

_exit:
  if (wkb) {
    GEOSFree(wkb);
  }
  if (writer) {
    GEOSWKBWriter_destroy(writer);
  }
  if (geom) {
    GEOSGeom_destroy(geom);
  }
  finishGEOS();

  return code;
}

int32_t geomFromTextFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  int32_t code = TSDB_CODE_SUCCESS;

  SColumnInfoData *pInputData = pInput->columnData;
  SColumnInfoData *pOutputData = pOutput->columnData;

  char *output = taosMemoryCalloc(TSDB_MAX_GEOMETRY_LEN, 1);

  for (int32_t i = 0; i < pInput->numOfRows; ++i) {
    if (colDataIsNull_s(pInputData, i)) {
      colDataAppendNULL(pOutputData, i);
      continue;
    }

    char *input = colDataGetData(pInputData, i);
    code = geomFromText(input, output);
    if (code != TSDB_CODE_SUCCESS) {
      goto _exit;
    }

    colDataAppend(pOutputData, i, output, false);
  }

  pOutput->numOfRows = pInput->numOfRows;

_exit:
  taosMemoryFree(output);

  return code;
}

int32_t asTextFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  int32_t code = TSDB_CODE_SUCCESS;

  SColumnInfoData *pInputData = pInput->columnData;
  SColumnInfoData *pOutputData = pOutput->columnData;

  char *output = taosMemoryCalloc(TSDB_MAX_BINARY_LEN, 1);

  for (int32_t i = 0; i < pInput->numOfRows; ++i) {
    if (colDataIsNull_s(pInputData, i)) {
      colDataAppendNULL(pOutputData, i);
      continue;
    }

    char *input = colDataGetData(pInputData, i);
    code = asText(input, output);
    if (code != TSDB_CODE_SUCCESS) {
      goto _exit;
    }

    colDataAppend(pOutputData, i, output, false);
  }

  pOutput->numOfRows = pInput->numOfRows;

_exit:
  taosMemoryFree(output);

  return code;
}

int32_t makePointFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
  int32_t code = TSDB_CODE_SUCCESS;

  SColumnInfoData *pInputData[inputNum];
  SColumnInfoData *pOutputData = pOutput->columnData;
  _getDoubleValue_fn_t getValueFn[inputNum];

  for (int32_t i = 0; i < inputNum; ++i) {
    pInputData[i] = pInput[i].columnData;
    getValueFn[i]= getVectorDoubleValueFn(GET_PARAM_TYPE(&pInput[i]));
  }

  char *output = taosMemoryCalloc(TSDB_MAX_GEOMETRY_LEN, 1);

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

      code = makePoint(getValueFn[0](pInputData[0]->pData, i), getValueFn[1](pInputData[1]->pData, i), output);
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

        code = makePoint(getValueFn[0](pInputData[0]->pData, 0), getValueFn[1](pInputData[1]->pData, i), output);
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

        code = makePoint(getValueFn[0](pInputData[0]->pData, i), getValueFn[1](pInputData[1]->pData, 0), output);
        if (code != TSDB_CODE_SUCCESS) {
          goto _exit;
        }
        colDataAppend(pOutputData, i, output, false);
      }
    }
  }

  pOutput->numOfRows = numOfRows;

_exit:
  taosMemoryFree(output);

  return code;
}
