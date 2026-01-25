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
    int32_t numOfCols = reader->oriHeader->numOfCols;
    
    reader->lenPtr  = (char *)blockData + sizeof(oriBlockHeader);     // header
    reader->lenPtr += numOfCols * (sizeof(int8_t) + sizeof(int32_t)); // schema
    reader->dataPtr = reader->lenPtr + sizeof(int32_t) * numOfCols;   // columns lengths
    reader->offsetLen  = 0;
    reader->offsetData = 0;

    return TSDB_CODE_SUCCESS;
}

int32_t getColumnData(BlockReader* reader, void** colData, int32_t* colDataLen) {
    // get column length
    *colDataLen = *(int32_t*)(reader->lenPtr + reader->offsetLen);
    *colData = reader->dataPtr + reader->offsetData;

    // move next
    reader->offsetLen  += sizeof(int32_t);
    reader->offsetData += *colDataLen;
    
    return TSDB_CODE_SUCCESS;
}