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
CompressBlock* compressBlock(void *block, int blockRows, TAOS_FIELD* fields, int numFields, int *code) {
    CompressBlock* compressBlock = (CompressBlock*)calloc(1, sizeof(CompressBlock));

    // one stage encode

    // two stage compress

    return compressBlock;
}

// free compress data
void freeCompressData(CompressBlock* compressBlock) {
    if (compressBlock == NULL) {
        return;
    }

    if (compressBlock->data) {
        free(compressBlock->data);
    }

    free(compressBlock);
}   