#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "taosudf.h"

DLL_EXPORT int32_t l2norm_init() { return 0; }

DLL_EXPORT int32_t l2norm_destroy() { return 0; }

DLL_EXPORT int32_t l2norm_start(SUdfInterBuf* buf) {
  int32_t bufLen = sizeof(double);
  if (buf->bufLen < bufLen) {
    udfError("failed to execute udf since input buflen:%d < %d", buf->bufLen, bufLen);
    return TSDB_CODE_UDF_INVALID_BUFSIZE;
  }

  udfTrace("start aggregation, buflen:%d used:%d", buf->bufLen, bufLen);
  *(int64_t*)(buf->buf) = 0;
  buf->bufLen = bufLen;
  buf->numOfResult = 0;
  return 0;
}

DLL_EXPORT int32_t l2norm(SUdfDataBlock* block, SUdfInterBuf* interBuf, SUdfInterBuf* newInterBuf) {
  udfTrace("block:%p, processing begins, cols:%d rows:%d", block, block->numOfCols, block->numOfRows);

  for (int32_t i = 0; i < block->numOfCols; ++i) {
    SUdfColumn* col = block->udfCols[i];
    if (col->colMeta.type != TSDB_DATA_TYPE_INT && col->colMeta.type != TSDB_DATA_TYPE_DOUBLE) {
      udfError("block:%p, col:%d type:%d should be int(%d) or double(%d)", block, i, col->colMeta.type,
               TSDB_DATA_TYPE_INT, TSDB_DATA_TYPE_DOUBLE);
      return TSDB_CODE_UDF_INVALID_INPUT;
    }
  }

  double sumSquares = *(double*)interBuf->buf;
  int8_t numNotNull = 0;

  for (int32_t i = 0; i < block->numOfCols; ++i) {
    for (int32_t j = 0; j < block->numOfRows; ++j) {
      SUdfColumn* col = block->udfCols[i];
      if (udfColDataIsNull(col, j)) {
        udfTrace("block:%p, col:%d row:%d is null", block, i, j);
        continue;
      }

      switch (col->colMeta.type) {
        case TSDB_DATA_TYPE_INT: {
          char*   cell = udfColDataGetData(col, j);
          int32_t num = *(int32_t*)cell;
          sumSquares += (double)num * num;
          udfTrace("block:%p, col:%d row:%d data:%d", block, i, j, num);
          break;
        }
        case TSDB_DATA_TYPE_DOUBLE: {
          char*  cell = udfColDataGetData(col, j);
          double num = *(double*)cell;
          sumSquares += num * num;
          udfTrace("block:%p, col:%d row:%d data:%f", block, i, j, num);
          break;
        }
        default:
          break;
      }
      ++numNotNull;
    }
    udfTrace("block:%p, col:%d result is %f", block, i, sumSquares);
  }

  *(double*)(newInterBuf->buf) = sumSquares;
  newInterBuf->bufLen = sizeof(double);
  newInterBuf->numOfResult = 1;

  udfTrace("block:%p, result is %f", block, sumSquares);
  return 0;
}

DLL_EXPORT int32_t l2norm_finish(SUdfInterBuf* buf, SUdfInterBuf* resultData) {
  double sumSquares = *(double*)(buf->buf);
  *(double*)(resultData->buf) = sqrt(sumSquares);
  resultData->bufLen = sizeof(double);
  resultData->numOfResult = 1;

  udfTrace("end aggregation, result is %f", *(double*)(resultData->buf));
  return 0;
}
