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
    
#include "storageTaos.h"
#include "colCompress.h"

TaosFile* createTaosFile(const char *fileName) {
    TaosFile* taosFile = (TaosFile*)malloc(sizeof(TaosFile));
    if (taosFile == NULL) {
        logError("malloc TaosFile failed");
        return NULL;
    }

    taosFile->fileName = fileName;
    if (taosFile->fileName == NULL) {
        logError("strdup fileName failed: %s", fileName);
        free(taosFile);
        return NULL;
    }

    // create
    taosFile->fp = fopen(fileName, "wb");
    if (taosFile->fp == NULL) {
        logError("fopen file failed: %s", fileName);
        free(taosFile);
        return NULL;
    }

    // write header TODO

    return taosFile;
}

void closeTaosFile(TaosFile* taosFile) {
    if (taosFile == NULL) {
        return;
    }

    // write footer TODO

    if (taosFile->fp) {
        fclose(taosFile->fp);
    }

    free(taosFile);
}


//
// write block to taos file
//
int writeBlockToTaosFile(TaosFile* taosFile, void *block, int blockRows, TAOS_FIELD* fields, int numFields) {
    int code = TSDB_CODE_FAILED;
    // compress block
    CompressData* compressData = compressBlock(block, blockRows, fields, numFields, &code);
    if (code != TSDB_CODE_SUCCESS) {
        logError("compress block failed: %d", code);
        return code;
    }

    // write to file
    size_t writeLen = fwrite(compressData->data, 1, compressData->len, taosFile->fp);
    if (writeLen != (size_t)compressData->len) {
        logError("write to file failed, writeLen: %zu, expectLen: %d", writeLen, compressData->len);
        code = TSDB_CODE_BCK_WRITE_FILE_FAILED;
        // free
        freeCompressData(compressData);
        return code;
    }

    // free
    freeCompressData(compressData);

    return TSDB_CODE_SUCCESS;
}


//
// write block to taos binary file
//
int resultToFileTaos(TAOS_RES *res, const char *fileName) {
    int code = TSDB_CODE_FAILED;

    int numFields = taos_num_fields(res);
    if (numFields <= 0) {
        logError("fields num is zero. errstr: %s", taos_errstr(res));
        return TSDB_CODE_BCK_NO_FIELDS;
    }
    TAOS_FIELD* fields = taos_fetch_fields(res);
    if (fields == NULL) {
        logError("fetch fields failed! errstr: %s", taos_errstr(res));
        return TSDB_CODE_BCK_FETCH_FIELDS_FAILED;
    }

    // create file
    TaosFile* taosFile = createTaosFile(fileName);
    if (taosFile == NULL) {
        logError("create Taos file failed: %s", fileName);
        return TSDB_CODE_BCK_CREATE_FILE_FAILED;
    }

    // while fetch data
    int numRows = 0;
    int nBlocks = 0;
    int blockRows = 0;
    void *block = NULL;
    while (taos_fetch_raw_block(res, &blockRows, &block) == TSDB_CODE_SUCCESS) {
        if (blockRows == 0 || block == NULL) {
            continue;
        }
        // write block to file
        code = writeBlockToTaosFile(taosFile, block, blockRows, fields, numFields);
        
        if (code != TSDB_CODE_SUCCESS) {
            logError("write data block to file failed(%d): %s", code, fileName);
            taos_free_result(res);
            return code;
        }
        numRows += blockRows;
        nBlocks++;
    }

    taosFile->nBlocks = nBlocks;
    closeTaosFile(taosFile);

    return code;
}
