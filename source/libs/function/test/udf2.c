#include <string.h>
#include <stdlib.h>
#include <stdio.h>

#include "tudf.h"

#undef malloc
#define malloc malloc
#undef free
#define free free

int32_t udf2_init() {
  return 0;
}

int32_t udf2_destroy() {
  return 0;
}

int32_t udf2_start(SUdfInterBuf *buf) {
  *(int64_t*)(buf->buf) = 0;
  buf->bufLen = sizeof(int64_t);
  buf->numOfResult = 0;
  return 0;
}

int32_t udf2(SUdfDataBlock* block, SUdfInterBuf *interBuf, SUdfInterBuf *newInterBuf) {
  int64_t sumSquares = *(int64_t*)interBuf->buf;
  int8_t numOutput = 0;
  for (int32_t i = 0; i < block->numOfCols; ++i) {
    SUdfColumn* col = block->udfCols[i];
    if (col->colMeta.type != TSDB_DATA_TYPE_INT) {
      return TSDB_CODE_UDF_INVALID_INPUT;
    }
  }
  for (int32_t i = 0; i < block->numOfCols; ++i) {
    for (int32_t j = 0; j < block->numOfRows; ++j) {
      SUdfColumn* col = block->udfCols[i];
      if (udfColDataIsNull(col, j)) {
        continue;
      }

      char* cell = udfColDataGetData(col, j);
      int32_t num = *(int32_t*)cell;
      sumSquares += num * num;
      numOutput = 1;
    }
  }

  if (numOutput == 1) {
    *(int64_t*)(newInterBuf->buf) = sumSquares;
    newInterBuf->bufLen = sizeof(int64_t);
  }
  newInterBuf->numOfResult = numOutput;
  return 0;
}

int32_t udf2_finish(SUdfInterBuf* buf, SUdfInterBuf *resultData) {
  if (buf->numOfResult == 0) {
    resultData->numOfResult = 0;
    return 0;
  }
  int64_t sumSquares = *(int64_t*)(buf->buf);
  *(double*)(resultData->buf) = sqrt(sumSquares);
  resultData->bufLen = sizeof(double);
  resultData->numOfResult = 1;
  return 0;
}
