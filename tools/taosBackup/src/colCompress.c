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
    
#include "colCompress.h"


//
// compress block
//
CompressData* compressBlock(void *block, int blockRows, TAOS_FIELD* fields, int numFields, int *code) {
    CompressData* compressData = (CompressData*)calloc(1, sizeof(CompressData));

    // one stage encode

    // two stage compress

    return compressData;
}

// free compress data
void freeCompressData(CompressData* compressData) {
    if (compressData == NULL) {
        return;
    }

    if (compressData->data) {
        free(compressData->data);
    }

    free(compressData);
}   