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
#include "blockReader.h"
// engine
#include "tcompression.h"
#include "tcol.h"

void fillFieldsInfo(FieldInfo* fieldInfos, TAOS_FIELD* fields, int numFields) {
    for (int i = 0; i < numFields; i++) {
        fieldInfos[i].type = fields[i].type;
        fieldInfos[i].bytes = fields[i].bytes;
        // default from engine
        fieldInfos[i].encode = getDefaultEncode(fields[i].type);
        fieldInfos[i].compress = getDefaultCompress(fields[i].type);
        fieldInfos[i].level = getDefaultCompressLevel(fields[i].type);
    }
}

uint32_t calcCompressBlockSize(BlockReader* reader) {
    uint32_t size = sizeof(CompressBlock);
    // ori header size
    size += sizeof(oriBlockHeader);
    
    // data size
    size += reader->oriHeader->actualLen;

    // add 20% margin
    size += size * 0.2;

    return size;
}

//
// compress block
//
CompressBlock* compressBlock(void *block, int blockRows, FieldInfo* fieldInfos, int numFields, int *code) {

    // read block
    BlockReader reader;
    int32_t retCode = initBlockReader(&reader, block);
    if (retCode != TSDB_CODE_SUCCESS) {
        *code = retCode;
        return NULL;
    }

    // malloc compress block
    uint32_t compressBlockLen = calcCompressBlockSize(&reader);
    CompressBlock* compressBlock = (CompressBlock*)calloc(1, compressBlockLen);
    if (compressBlock == NULL) {
        logError("malloc compress block failed");
        *code = TSDB_CODE_BCK_MALLOC_FAILED;
        return NULL;
    }

    //
    // header
    //
    compressBlock->version = COMPRESS_BLOCK_VERSION;
    compressBlock->flag = 0; // no use

    // fill original block header
    compressBlock->oriHeader = *(reader.oriHeader);

    //
    // body
    //
    int32_t *colsLen = (int32_t *)compressBlock->data;
    char* colsDataStart = compressBlock->data + sizeof(int32_t) * numFields;
    char* curDataPos = colsDataStart;

    // loop columns
    for (int i = 0; i < numFields; i++) {
        // get column data
        void* colData = NULL;
        int32_t colDataLen = 0;
        retCode = taosGetColumnData(&reader, i, &colData, &colDataLen);
        if (retCode != TSDB_CODE_SUCCESS) {
            logError("get column data failed: %d", retCode);
            freeCompressData(compressBlock);
            *code = retCode;
            return NULL;
        }

        // compress column data
        int32_t compressedLen = compressBuffer(fieldInfos[i].compress, fieldInfos[i].level,
                                               colData, colDataLen,
                                               curDataPos, compressBlockLen - (curDataPos - compressBlock->data));
        if (compressedLen < 0) {
            logError("compress column data failed: %d", retCode);
            freeCompressData(compressBlock);
            *code = TSDB_CODE_BCK_COMPRESS_FAILED;
            return NULL;
        }

        // set column len
        colsLen[i] = compressedLen;

        // move current data pos
        curDataPos += compressedLen;
    }

    *code = TSDB_CODE_SUCCESS;
    return compressBlock;
}

// free compress data
void freeCompressData(CompressBlock* compressBlock) {
    if (compressBlock == NULL) {
        return;
    }

    taosMemFree(compressBlock);
}   