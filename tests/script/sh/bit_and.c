#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include "taosudf.h"


DLL_EXPORT int32_t bit_and_init() {
    return 0;
}

DLL_EXPORT int32_t bit_and_destroy() {
    return 0;
}

DLL_EXPORT int32_t bit_and(SUdfDataBlock* block, SUdfColumn *resultCol) {

    if (block->numOfCols < 2) {
        return TSDB_CODE_UDF_INVALID_INPUT;
    }

    for (int32_t i = 0; i < block->numOfCols; ++i) {
        SUdfColumn* col = block->udfCols[i];
        if (!(col->colMeta.type == TSDB_DATA_TYPE_INT)) {
            return TSDB_CODE_UDF_INVALID_INPUT;
        }
    }

    SUdfColumnMeta *meta = &resultCol->colMeta;
    meta->bytes = 4;
    meta->type = TSDB_DATA_TYPE_INT;
    meta->scale = 0;
    meta->precision = 0;


    SUdfColumnData *resultData = &resultCol->colData;

    resultData->numOfRows = block->numOfRows;

    for (int32_t i = 0; i < resultData->numOfRows; ++i) {
        if (udfColDataIsNull(block->udfCols[0], i)) {
            udfColDataSetNull(resultCol, i);
            continue;
        }
        int32_t result = *(int32_t*)udfColDataGetData(block->udfCols[0], i);
        int j = 1;
        for (; j < block->numOfCols; ++j) {
            if (udfColDataIsNull(block->udfCols[j], i)) {
                udfColDataSetNull(resultCol, i);
                break;
            }

            char* colData = udfColDataGetData(block->udfCols[j], i);
            result &= *(int32_t*)colData;
        }
        if (j == block->numOfCols) {
            udfColDataSet(resultCol, i, (char*)&result, false);
        }

    }
    return TSDB_CODE_SUCCESS;
}
