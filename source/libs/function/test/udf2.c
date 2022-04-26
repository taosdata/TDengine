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

int32_t udf2(SUdfDataBlock* block, SUdfInterBuf *interBuf) {
  int64_t sumSquares = *(int64_t*)interBuf->buf;
  for (int32_t i = 0; i < block->numOfCols; ++i) {
    for (int32_t j = 0; j < block->numOfRows; ++i) {
      SUdfColumn* col = block->udfCols[i];
      //TODO: check the bitmap for null value
      int32_t* rows = (int32_t*)col->colData.fixLenCol.data;
      sumSquares += rows[j] * rows[j];
    }
  }

  *(int64_t*)interBuf = sumSquares;
  interBuf->bufLen = sizeof(int64_t);
  //TODO: if all null value, numOfResult = 0;
  interBuf->numOfResult = 1;
  return 0;
}

int32_t udf2_finish(SUdfInterBuf* buf, SUdfInterBuf *resultData) {
  //TODO: check numOfResults;
  int64_t sumSquares = *(int64_t*)(buf->buf);
  *(double*)(resultData->buf) = sqrt(sumSquares);
  resultData->bufLen = sizeof(double);
  resultData->numOfResult = 1;
  return 0;
}
