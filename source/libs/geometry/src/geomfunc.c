#include <geos_c.h>
#include "geomfunc.h"
#include "querynodes.h"
#include "tdatablock.h"
#include "sclInt.h"
#include "sclvector.h"

static void makepoint(double x, double y, unsigned char *output) {
  initGEOS(0, 0);

  GEOSGeometry *geom = GEOSGeom_createPointFromXY(x, y);

  //GEOSWKBWriter *writer = GEOSWKBWriter_create();
  //size_t size = 0;
  //unsigned char *geosOutput = GEOSWKBWriter_write(writer, geom, &size);

  GEOSWKTWriter *writer = GEOSWKTWriter_create();
  unsigned char *geosOutput = GEOSWKTWriter_write(writer, geom);
  size_t size = strlen(geosOutput);

  memcpy(varDataVal(output), geosOutput, size);
  varDataSetLen(output, size);

  GEOSFree(geosOutput);
  GEOSWKTWriter_destroy(writer);

  finishGEOS();
}

int32_t makepointFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput) {
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

      makepoint(getValueFn[0](pInputData[0]->pData, i), getValueFn[1](pInputData[1]->pData, i), output);
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

        makepoint(getValueFn[0](pInputData[0]->pData, 0), getValueFn[1](pInputData[1]->pData, i), output);
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

        makepoint(getValueFn[0](pInputData[0]->pData, i), getValueFn[1](pInputData[1]->pData, 0), output);
        colDataAppend(pOutputData, i, output, false);
      }
    }
  }

  pOutput->numOfRows = numOfRows;
  taosMemoryFree(output);

  return TSDB_CODE_SUCCESS;
}
