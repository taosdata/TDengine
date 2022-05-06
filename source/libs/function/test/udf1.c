#include <string.h>
#include <stdlib.h>
#include <stdio.h>

#include "tudf.h"

#undef malloc
#define malloc malloc
#undef free
#define free free

int32_t udf1_init() {
  return 0;
}

int32_t udf1_destroy() {
  return 0;
}

int32_t udf1(SUdfDataBlock* block, SUdfColumn *resultCol) {
  SUdfColumnMeta *meta = &resultCol->colMeta;
  meta->bytes = 4;
  meta->type = TSDB_DATA_TYPE_INT;
  meta->scale = 0;
  meta->precision = 0;

  SUdfColumnData *resultData = &resultCol->colData;
  resultData->numOfRows = block->numOfRows;
  SUdfColumnData *srcData = &block->udfCols[0]->colData;

  for (int32_t i = 0; i < resultData->numOfRows; ++i) {
    int32_t luckyNum = 88;
    udfColSetRow(resultCol, i, (char*)&luckyNum, false);
  }

  return 0;
}