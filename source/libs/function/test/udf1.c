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
  SUdfColumnData *resultData = &resultCol->colData;
  resultData->numOfRows = block->numOfRows;
  SUdfColumnData *srcData = &block->udfCols[0]->colData;
  resultData->varLengthColumn = srcData->varLengthColumn;

  if (resultData->varLengthColumn) {
    resultData->varLenCol.varOffsetsLen = srcData->varLenCol.varOffsetsLen;
    resultData->varLenCol.varOffsets = malloc(resultData->varLenCol.varOffsetsLen);
    memcpy(resultData->varLenCol.varOffsets, srcData->varLenCol.varOffsets, srcData->varLenCol.varOffsetsLen);

    resultData->varLenCol.payloadLen = srcData->varLenCol.payloadLen;
    resultData->varLenCol.payload = malloc(resultData->varLenCol.payloadLen);
    memcpy(resultData->varLenCol.payload, srcData->varLenCol.payload, srcData->varLenCol.payloadLen);
  } else {
    resultData->fixLenCol.nullBitmapLen = srcData->fixLenCol.nullBitmapLen;
    resultData->fixLenCol.nullBitmap = malloc(resultData->fixLenCol.nullBitmapLen);
    memcpy(resultData->fixLenCol.nullBitmap, srcData->fixLenCol.nullBitmap, srcData->fixLenCol.nullBitmapLen);

    resultData->fixLenCol.dataLen = srcData->fixLenCol.dataLen;
    resultData->fixLenCol.data = malloc(resultData->fixLenCol.dataLen);
    memcpy(resultData->fixLenCol.data, srcData->fixLenCol.data, srcData->fixLenCol.dataLen);
    for (int32_t i = 0; i < resultData->numOfRows; ++i) {
      *(resultData->fixLenCol.data + i * sizeof(int32_t)) = 88;
    }
  }

  SUdfColumnMeta *meta = &resultCol->colMeta;
  meta->bytes = 4;
  meta->type = TSDB_DATA_TYPE_INT;
  meta->scale = 0;
  meta->precision = 0;
  return 0;
}

int32_t udf1_free(SUdfColumn *col) {
  SUdfColumnData *data = &col->colData;
  if (data->varLengthColumn) {
    free(data->varLenCol.varOffsets);
    data->varLenCol.varOffsets = NULL;
    free(data->varLenCol.payload);
    data->varLenCol.payload = NULL;
  } else {
    free(data->fixLenCol.nullBitmap);
    data->fixLenCol.nullBitmap = NULL;
    free(data->fixLenCol.data);
    data->fixLenCol.data = NULL;
  }
  return 0;
}