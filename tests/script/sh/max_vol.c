#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <math.h>

#include "taosudf.h"

#define STR_MAX_LEN 256 // inter buffer length

// init
DLL_EXPORT int32_t max_vol_init()
{
    return 0;
}

// destory
DLL_EXPORT int32_t max_vol_destroy()
{
    return 0;
}

// start 
DLL_EXPORT int32_t max_vol_start(SUdfInterBuf *buf)
{
    memset(buf->buf, 0, sizeof(float) + STR_MAX_LEN);
    // set init value
    *((float*)buf->buf) = -10000000;
    buf->bufLen = sizeof(float)  + STR_MAX_LEN;
    buf->numOfResult = 0;
    return 0;
}

DLL_EXPORT int32_t max_vol(SUdfDataBlock *block, SUdfInterBuf *interBuf, SUdfInterBuf *newInterBuf) {
    float maxValue = *(float *)interBuf->buf;
    char strBuff[STR_MAX_LEN] = "inter1buf";
    
    if (block->numOfCols < 2)
    {
        return TSDB_CODE_UDF_INVALID_INPUT;
    }

    // check data type
    for (int32_t i = 0; i < block->numOfCols; ++i)
    {
        SUdfColumn *col = block->udfCols[i];
        if( i == block->numOfCols - 1) {
          // last column is device id , must varchar
          if (col->colMeta.type != TSDB_DATA_TYPE_VARCHAR ) {
            return TSDB_CODE_UDF_INVALID_INPUT;
          }
        } else {
          if (col->colMeta.type != TSDB_DATA_TYPE_FLOAT) {
            return TSDB_CODE_UDF_INVALID_INPUT;
          }
        }
    }

    // calc max voltage
    SUdfColumn *lastCol = block->udfCols[block->numOfCols - 1];
    for (int32_t i = 0; i < (block->numOfCols - 1); ++i) {
        for (int32_t j = 0; j < block->numOfRows; ++j) {
          SUdfColumn *col = block->udfCols[i];
          if (udfColDataIsNull(col, j)) {
            continue;
          }
          char *data = udfColDataGetData(col, j);
          float voltage = *(float *)data;
          if (voltage > maxValue) {
            maxValue = voltage;
            char *valData = udfColDataGetData(lastCol, j);
            // get device id
            char *deviceId = valData + sizeof(uint16_t);
            sprintf(strBuff, "%s_(%d,%d)_%f", deviceId, j, i, maxValue);
          }
        }
    }

    *(float*)newInterBuf->buf = maxValue;
    strcpy(newInterBuf->buf + sizeof(float), strBuff);
    newInterBuf->bufLen = sizeof(float) + strlen(strBuff)+1;
    newInterBuf->numOfResult = 1;
    return 0;
}

DLL_EXPORT int32_t max_vol_finish(SUdfInterBuf *buf, SUdfInterBuf *resultData)
{
    char * str = buf->buf + sizeof(float);
    // copy to des
    char * des = resultData->buf + sizeof(uint16_t);
    strcpy(des, str);

    // set binary type len
    uint16_t  len = strlen(str);
    *((uint16_t*)resultData->buf) = len;

    // set buf len
    resultData->bufLen = len + sizeof(uint16_t);
    // set row count
    resultData->numOfResult = 1;
    return 0;
}
