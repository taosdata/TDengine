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

int writeTaosFile(TaosFile* taosFile, void *data, int len) {
    if (taosFile == NULL || taosFile->fp == NULL) {
        logError("invalid TaosFile");
        return TSDB_CODE_BCK_WRITE_FILE_FAILED;
    }

    size_t writeLen = fwrite(data, 1, len, taosFile->fp);
    if (writeLen != (size_t)len) {
        logError("write to file failed, writeLen: %zu, expectLen: %d", writeLen, len);
        return TSDB_CODE_BCK_WRITE_FILE_FAILED;
    }

    return TSDB_CODE_SUCCESS;
}

TaosFile* createTaosFile(const char *fileName, int *code) {
    TaosFile* taosFile = (TaosFile*)malloc(sizeof(TaosFile));
    memset(taosFile, 0, sizeof(TaosFile));
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

    // write header
    memcpy(taosFile->header.magic, TAOSFILE_MAGIC, 4);
    taosFile->header.version = TAOSFILE_VERSION;

    *code = writeTaosFile(taosFile, &taosFile->header, sizeof(TaosFileHeader));
    if (*code != TSDB_CODE_SUCCESS) {
        closeTaosFile(taosFile);
        return NULL;
    }

    return taosFile;
}

int closeTaosFile(TaosFile* taosFile) {
    if (taosFile == NULL) {
        return TSDB_CODE_BCK_INVALID_PARAM;
    }

    if (taosFile->fp) {
        // update header
        fseek(taosFile->fp, 0, SEEK_SET);
        int code = writeTaosFile(taosFile, &taosFile->header, sizeof(TaosFileHeader));
        if (code != TSDB_CODE_SUCCESS) {
            fclose(taosFile->fp);
            free(taosFile);
            logError("update taos file header failed: %s", taosFile->fileName);
            return code;
        }
        // close file
        fclose(taosFile->fp);
    }

    // free memory
    free(taosFile);

    return TSDB_CODE_SUCCESS;
}

//
// write block to taos file
//
int writeBlockToTaosFile(TaosFile* taosFile, void *block, int blockRows, TAOS_FIELD* fields, int numFields) {
    int code = TSDB_CODE_FAILED;
    // compress block
    CompressBlock* compBlock = compressBlock(block, blockRows, fields, numFields, &code);
    if (code != TSDB_CODE_SUCCESS) {
        logError("compress block failed: %d", code);
        return code;
    }

    // write to file
    code = writeTaosFile(taosFile, compBlock, sizeof(CompressBlock) + compBlock->dataLen);
    if (code != TSDB_CODE_SUCCESS) {
        // free
        freeCompressData(compBlock);
        return code;
    }

    // free
    freeCompressData(compBlock);

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
    TaosFile* taosFile = createTaosFile(fileName, &code);
    if (taosFile == NULL) {
        logError("create Taos file failed: %s", fileName);
        return code;
    }

    // while fetch data
    int blockRows = 0;
    void *block = NULL;
    while (taos_fetch_raw_block(res, &blockRows, &block) == TSDB_CODE_SUCCESS) {
        if (blockRows == 0 || block == NULL) {
            continue;
        }
        // write block to file
        code = writeBlockToTaosFile(taosFile, block, blockRows, fields, numFields);        
        if (code != TSDB_CODE_SUCCESS) {
            // TODO ignore or failed
            logError("write data block to file failed(%d): %s", code, fileName);
            taos_free_result(res);
            code = closeTaosFile(taosFile);
            if (code != TSDB_CODE_SUCCESS) {
                logError("close Taos file failed(%d): %s", code, fileName);
                return code;
            }
            return code;
        }

        taosFile->header.nBlocks ++;
        taosFile->header.numRows += blockRows;
    }
    // free result
    taos_free_result(res);

    // close file
    code = closeTaosFile(taosFile);
    if (code != TSDB_CODE_SUCCESS) {
        logError("close Taos file failed(%d): %s", code, fileName);
        return code;
    }

    return TSDB_CODE_SUCCESS;
}
