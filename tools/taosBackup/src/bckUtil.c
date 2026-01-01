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
    
#include  "bck.h"
#include "bckUtil.h"

void sleepMs(int ms) {
    usleep(ms * 1000);
}

void freeArrayPtr(char **ptr) {
    if (ptr == NULL) {
        return;
    }

    for (int i = 0; ptr[i] != NULL; i++) {
        free(ptr[i]);
    }
    free(ptr);
}

void freePtr(void *ptr) {
    if (ptr != NULL) {
        free(ptr);
    }
}

bool errorCodeCanRetry(int code) {
    if (code == TSDB_CODE_RPC_NETWORK_ERROR ||
        code == TSDB_CODE_RPC_NETWORK_BUSY ||
        code == TSDB_CODE_RPC_TIMEOUT) {
        return true;
    }

    return false;
}

unsigned int getCrc(const char *name) {
    unsigned int crc = 0xFFFFFFFF;
    while (*name) {
        crc ^= (unsigned char)(*name++);
        for (int i = 0; i < 8; i++) {
            crc = (crc >> 1) ^ (0xEDB88320 & -(crc & 1));
        }
    }
    return ~crc;
}


int obtainFileName(BackFileType fileType, 
                      const char *dbName, 
                      const char *stbName,
                      const char *tbName,
                      int index,
                      int fileCount,
                      StorageFormat format,
                      char *fileName, 
                      int len) {

    char * outPath = argsOutPath();
    const char * ext = (format == BINARY_TAOS) ? "dat" : "parquet";

    switch (fileType)
    {
    case BACK_FILE_DBSQL:
        snprintf(fileName, len, "%s/db.sql", outPath, dbName);
        break;
    case BACK_FILE_STBJSON:
        snprintf(fileName, len, "%s/%s/schema.json", outPath, dbName, stbName);
        break;
    case BACK_FILE_TAG:
        snprintf(fileName, len, "%s/%s/tags/tag_%d.%s", outPath, dbName, stbName, index, ext);
        break;
    case BACK_FILE_DATA:
        snprintf(fileName, len, "%s/%s/data%d_%d/%s.%s", outPath, dbName, stbName, index, fileCount/FOLDER_MAXFILE + 1, tbName, ext);
        break;
    default:
        return TSDB_CODE_BACKUP_INVALID_PARAM;
    }
    
    return TSDB_CODE_SUCCESS;
}
