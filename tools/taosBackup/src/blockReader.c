/*
 * Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the MIT license as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 */
    
#include "blockReader.h"

int32_t initBlockReader(BlockReader* reader, void* blockData) {
    if (reader == NULL || blockData == NULL) {
        return TSDB_CODE_BCK_INVALID_PARAM;
    }

    reader->data = blockData;
    reader->offset = 0;

    return TSDB_CODE_SUCCESS;
}

int32_t taosGetColumnData(BlockReader* reader, int colIndex,  void** colData, int32_t* colDataLen) {
    if (reader == NULL || reader->oriHeader == NULL || colData == NULL || colDataLen == NULL) {
        return TSDB_CODE_BCK_INVALID_PARAM;
    }

    if (colIndex < 0 || colIndex >= reader->oriHeader->numOfCols) {
        return TSDB_CODE_BCK_INVALID_PARAM;
    }

    // calculate column offset
    int32_t offset = sizeof(oriBlockHeader);
    int32_t* colLens = (int32_t*)((char*)reader->data + offset);
    for (int i = 0; i < colIndex; i++) {
        offset += colLens[i];
    }

    // set column data and len
    *colData = (char*)reader->data + offset + sizeof(int32_t) * reader->oriHeader->numOfCols;
    *colDataLen = colLens[colIndex];

    return TSDB_CODE_SUCCESS;
}