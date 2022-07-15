#include <string.h>
#include <stdlib.h>
#include <stdio.h>

#include "taosudf.h"


DLL_EXPORT int32_t udf1_init() {
  return 0;
}

DLL_EXPORT int32_t udf1_destroy() {
  return 0;
}

DLL_EXPORT int32_t udf1(SUdfDataBlock* block, SUdfColumn *resultCol) {
  SUdfColumnMeta *meta = &resultCol->colMeta;
  meta->bytes = 4;
  meta->type = TSDB_DATA_TYPE_INT;
  meta->scale = 0;
  meta->precision = 0;

  SUdfColumnData *resultData = &resultCol->colData;
  resultData->numOfRows = block->numOfRows;
  for (int32_t i = 0; i < resultData->numOfRows; ++i) {
    int j = 0;
    for (; j < block->numOfCols; ++j) {
      if (udfColDataIsNull(block->udfCols[j], i)) {
        udfColDataSetNull(resultCol, i);
        break;
      }
    }
    if ( j == block->numOfCols) {
      int32_t luckyNum = 88;
      udfColDataSet(resultCol, i, (char *)&luckyNum, false);
    }
  }

  return 0;
}