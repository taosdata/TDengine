#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "taos.h"
#include "taoserror.h"
#include "taosudf.h"

// Define a structure to store sum and count
typedef struct {
    double sum;
    int count;
} SumCount;

// initialization function
DLL_EXPORT int32_t extract_avg_init() {
    udfTrace("extract_avg_init: Initializing UDF");
    return TSDB_CODE_SUCCESS;
}

DLL_EXPORT int32_t extract_avg_start(SUdfInterBuf *interBuf) {
    int32_t bufLen = sizeof(SumCount);
    if (interBuf->bufLen < bufLen) {
        udfError("extract_avg_start: Failed to execute UDF since input buflen:%d < %d", interBuf->bufLen, bufLen);
        return TSDB_CODE_UDF_INVALID_BUFSIZE;
      }
 
    // Initialize sum and count
    SumCount *sumCount = (SumCount *)interBuf->buf;
    sumCount->sum = 0.0;
    sumCount->count = 0;

    interBuf->numOfResult = 0;

    udfTrace("extract_avg_start: Initialized sum=0.0, count=0");
    return TSDB_CODE_SUCCESS;
}

DLL_EXPORT int32_t extract_avg(SUdfDataBlock *inputBlock, SUdfInterBuf *interBuf, SUdfInterBuf *newInterBuf) {
    udfTrace("extract_avg: Processing data block with %d rows", inputBlock->numOfRows);

    // Check the number of columns in the input data block
    if (inputBlock->numOfCols != 1) {
        udfError("extract_avg: Invalid number of columns. Expected 1, got %d", inputBlock->numOfCols);
        return TSDB_CODE_UDF_INVALID_INPUT;
    }

    // Get the input column
    SUdfColumn *inputCol = inputBlock->udfCols[0];

    if (inputCol->colMeta.type != TSDB_DATA_TYPE_VARCHAR) {
        udfError("extract_avg: Invalid data type. Expected VARCHAR, got %d", inputCol->colMeta.type);
        return TSDB_CODE_UDF_INVALID_INPUT;
    }

    // Read the current sum and count from interBuf
    SumCount *sumCount = (SumCount *)interBuf->buf;
    udfTrace("extract_avg: Starting with sum=%f, count=%d", sumCount->sum, sumCount->count);

    for (int i = 0; i < inputBlock->numOfRows; i++) {
        if (udfColDataIsNull(inputCol, i)) {
            udfTrace("extract_avg: Skipping NULL value at row %d", i);
            continue;
        }

        char *buf = (char *)udfColDataGetData(inputCol, i);

        char data[64];
        memset(data, 0, 64);
        memcpy(data, varDataVal(buf), varDataLen(buf));

        udfTrace("extract_avg: Processing row %d, data='%s'", i, data);

        char *rest = data;
        char *token;
        while ((token = strtok_r(rest, ",", &rest))) {
            while (*token == ' ') token++;
            int tokenLen = strlen(token);
            while (tokenLen > 0 && token[tokenLen - 1] == ' ') token[--tokenLen] = '\0';

            if (tokenLen == 0) {
                udfTrace("extract_avg: Empty string encountered at row %d", i);
                continue;
            }

            char *endPtr;
            double value = strtod(token, &endPtr);

            if (endPtr == token || *endPtr != '\0') {
                udfError("extract_avg: Failed to convert string '%s' to double at row %d", token, i);
                continue;
            }

            sumCount->sum += value;
            sumCount->count++;
            udfTrace("extract_avg: Updated sum=%f, count=%d", sumCount->sum, sumCount->count);
        }
    }

    newInterBuf->bufLen = sizeof(SumCount);
    newInterBuf->buf = (char *)malloc(newInterBuf->bufLen);
    if (newInterBuf->buf == NULL) {
        udfError("extract_avg: Failed to allocate memory for newInterBuf");
        return TSDB_CODE_UDF_INTERNAL_ERROR;
    }
    memcpy(newInterBuf->buf, sumCount, newInterBuf->bufLen);
    newInterBuf->numOfResult = 0;

    udfTrace("extract_avg: Final sum=%f, count=%d", sumCount->sum, sumCount->count);
    return TSDB_CODE_SUCCESS;
}

DLL_EXPORT int32_t extract_avg_finish(SUdfInterBuf *interBuf, SUdfInterBuf *result) {
    SumCount *sumCount = (SumCount *)interBuf->buf;

    double avg = (sumCount->count > 0) ? (sumCount->sum / sumCount->count) : 0.0;

    *(double *)result->buf = avg;
    result->bufLen = sizeof(double);
    result->numOfResult = sumCount->count > 0 ? 1 : 0;

    udfTrace("extract_avg_finish: Final result=%f (sum=%f, count=%d)", avg, sumCount->sum, sumCount->count);
    return TSDB_CODE_SUCCESS;
}

DLL_EXPORT int32_t extract_avg_destroy() {
    udfTrace("extract_avg_destroy: Cleaning up UDF");
    return TSDB_CODE_SUCCESS;
}
