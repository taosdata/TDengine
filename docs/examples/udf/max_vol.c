#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "taosudf.h"

#define STR_MAX_LEN 256  // inter buffer length

DLL_EXPORT int32_t max_vol_init() { return 0; }

DLL_EXPORT int32_t max_vol_destroy() { return 0; }

DLL_EXPORT int32_t max_vol_start(SUdfInterBuf *buf) {
  int32_t bufLen = sizeof(float) + STR_MAX_LEN;
  if (buf->bufLen < bufLen) {
    udfError("failed to execute udf since input buflen:%d < %d", buf->bufLen, bufLen);
    return TSDB_CODE_UDF_INVALID_BUFSIZE;
  }

  udfTrace("start aggregation, buflen:%d used:%d", buf->bufLen, bufLen);
  memset(buf->buf, 0, sizeof(float) + STR_MAX_LEN);
  *((float *)buf->buf) = INT32_MIN;
  buf->bufLen = bufLen;
  buf->numOfResult = 0;
  return 0;
}

DLL_EXPORT int32_t max_vol(SUdfDataBlock *block, SUdfInterBuf *interBuf, SUdfInterBuf *newInterBuf) {
  udfTrace("block:%p, processing begins, cols:%d rows:%d", block, block->numOfCols, block->numOfRows);

  float maxValue = *(float *)interBuf->buf;
  char  strBuff[STR_MAX_LEN] = "inter1buf";

  if (block->numOfCols < 2) {
    udfError("block:%p, cols:%d needs to be greater than 2", block, block->numOfCols);
    return TSDB_CODE_UDF_INVALID_INPUT;
  }

  // check data type
  for (int32_t i = 0; i < block->numOfCols; ++i) {
    SUdfColumn *col = block->udfCols[i];
    if (i == block->numOfCols - 1) {
      // last column is device id , must varchar
      if (col->colMeta.type != TSDB_DATA_TYPE_VARCHAR) {
        udfError("block:%p, col:%d type:%d should be varchar(%d)", block, i, col->colMeta.type, TSDB_DATA_TYPE_VARCHAR);
        return TSDB_CODE_UDF_INVALID_INPUT;
      }
    } else {
      if (col->colMeta.type != TSDB_DATA_TYPE_FLOAT) {
        udfError("block:%p, col:%d type:%d should be float(%d)", block, i, col->colMeta.type, TSDB_DATA_TYPE_FLOAT);
        return TSDB_CODE_UDF_INVALID_INPUT;
      }
    }
  }

  // calc max voltage
  SUdfColumn *lastCol = block->udfCols[block->numOfCols - 1];
  for (int32_t i = 0; i < block->numOfCols - 1; ++i) {
    for (int32_t j = 0; j < block->numOfRows; ++j) {
      SUdfColumn *col = block->udfCols[i];
      if (udfColDataIsNull(col, j)) {
        udfTrace("block:%p, col:%d row:%d is null", block, i, j);
        continue;
      }

      char *data = udfColDataGetData(col, j);
      float voltage = *(float *)data;

      if (voltage <= maxValue) {
        udfTrace("block:%p, col:%d row:%d data:%f", block, i, j, voltage);
      } else {
        maxValue = voltage;
        char   *valData = udfColDataGetData(lastCol, j);
        int32_t valDataLen = udfColDataGetDataLen(lastCol, j);

        // get device id
        char   *deviceId = valData + sizeof(uint16_t);
        int32_t deviceIdLen = valDataLen < (STR_MAX_LEN - 1) ? valDataLen : (STR_MAX_LEN - 1);

        strncpy(strBuff, deviceId, deviceIdLen);
        snprintf(strBuff + deviceIdLen, STR_MAX_LEN - deviceIdLen, "_(%d,%d)_%f", j, i, maxValue);
        udfTrace("block:%p, col:%d row:%d data:%f, as max_val:%s", block, i, j, voltage, strBuff);
      }
    }
  }

  *(float *)newInterBuf->buf = maxValue;
  strncpy(newInterBuf->buf + sizeof(float), strBuff, STR_MAX_LEN);
  newInterBuf->bufLen = sizeof(float) + strlen(strBuff) + 1;
  newInterBuf->numOfResult = 1;

  udfTrace("block:%p, result is %s", block, strBuff);
  return 0;
}

DLL_EXPORT int32_t max_vol_finish(SUdfInterBuf *buf, SUdfInterBuf *resultData) {
  char *str = buf->buf + sizeof(float);
  // copy to des
  char *des = resultData->buf + sizeof(uint16_t);
  strcpy(des, str);

  // set binary type len
  uint16_t len = strlen(str);
  *((uint16_t *)resultData->buf) = len;

  // set buf len
  resultData->bufLen = len + sizeof(uint16_t);
  // set row count
  resultData->numOfResult = 1;

  udfTrace("end aggregation, result is %s", str);
  return 0;
}
