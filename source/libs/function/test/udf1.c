#include <string.h>
#include <stdlib.h>
#include <stdio.h>

#include "tudf.h"

#undef malloc
#define malloc malloc
#undef free
#define free free

int32_t udf1_setup() {
  return 0;
}

int32_t udf1_teardown() {
  return 0;
}

int32_t udf1(SUdfDataBlock block, SUdfColumnData *resultData) {

  resultData->numOfRows = block.numOfRows;
  SUdfColumnData *srcData = &block.udfCols[0]->colData;
  resultData->varLengthColumn = srcData->varLengthColumn;

  if (resultData->varLengthColumn) {
    resultData->varOffsetsLen = srcData->varOffsetsLen;
    resultData->varOffsets = malloc(resultData->varOffsetsLen);
    memcpy(resultData->varOffsets, srcData->varOffsets, srcData->varOffsetsLen);

    resultData->payloadLen = srcData->payloadLen;
    resultData->payload = malloc(resultData->payloadLen);
    memcpy(resultData->payload, srcData->payload, srcData->payloadLen);
  } else {
    resultData->nullBitmapLen = srcData->nullBitmapLen;
    resultData->nullBitmap = malloc(resultData->nullBitmapLen);
    memcpy(resultData->nullBitmap, srcData->nullBitmap, srcData->nullBitmapLen);

    resultData->dataLen = srcData->dataLen;
    resultData->data = malloc(resultData->dataLen);
    memcpy(resultData->data, srcData->data, srcData->dataLen);
  }

  return 0;
}

int32_t udf1_free(SUdfColumnData *data) {
  if (data->varLengthColumn) {
    free(data->varOffsets);
    data->varOffsets = NULL;
    free(data->payload);
    data->payload = NULL;
  } else {
    free(data->nullBitmap);
    data->nullBitmap = NULL;
    free(data->data);
    data->data = NULL;
  }
  return 0;
}