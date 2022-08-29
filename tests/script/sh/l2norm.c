#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <math.h>

#include "taosudf.h"

DLL_EXPORT int32_t l2norm_init() {
  return 0;
}

DLL_EXPORT int32_t l2norm_destroy() {
  return 0;
}

DLL_EXPORT int32_t l2norm_start(SUdfInterBuf *buf) {
  *(int64_t*)(buf->buf) = 0;
  buf->bufLen = sizeof(double);
  buf->numOfResult = 0;
  return 0;
}

DLL_EXPORT int32_t l2norm(SUdfDataBlock* block, SUdfInterBuf *interBuf, SUdfInterBuf *newInterBuf) {
  double sumSquares = *(double*)interBuf->buf;
  int8_t numNotNull = 0;
  for (int32_t i = 0; i < block->numOfCols; ++i) {
    SUdfColumn* col = block->udfCols[i];
    if (!(col->colMeta.type == TSDB_DATA_TYPE_INT || 
          col->colMeta.type == TSDB_DATA_TYPE_DOUBLE)) {
      return TSDB_CODE_UDF_INVALID_INPUT;
    }
  }
  for (int32_t i = 0; i < block->numOfCols; ++i) {
    for (int32_t j = 0; j < block->numOfRows; ++j) {
      SUdfColumn* col = block->udfCols[i];
      if (udfColDataIsNull(col, j)) {
        continue;
      }
      switch (col->colMeta.type) {
        case TSDB_DATA_TYPE_INT: {
          char* cell = udfColDataGetData(col, j);
          int32_t num = *(int32_t*)cell;
          sumSquares += (double)num * num;
          break;
        }
        case TSDB_DATA_TYPE_DOUBLE: {
          char* cell = udfColDataGetData(col, j);
          double num = *(double*)cell;
          sumSquares += num * num;
          break;
        }
        default: 
          break;
      }
      ++numNotNull;
    }
  }

  *(double*)(newInterBuf->buf) = sumSquares;
  newInterBuf->bufLen = sizeof(double);

  if (interBuf->numOfResult == 0 && numNotNull == 0) {
    newInterBuf->numOfResult = 0;
  } else {
    newInterBuf->numOfResult = 1;
  }
  return 0;
}

DLL_EXPORT int32_t l2norm_finish(SUdfInterBuf* buf, SUdfInterBuf *resultData) {
  if (buf->numOfResult == 0) {
    resultData->numOfResult = 0;
    return 0;
  }
  double sumSquares = *(double*)(buf->buf);
  *(double*)(resultData->buf) = sqrt(sumSquares);
  resultData->bufLen = sizeof(double);
  resultData->numOfResult = 1;
  return 0;
}
