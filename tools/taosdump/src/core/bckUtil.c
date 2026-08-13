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
#include "bckArgs.h"

void sleepMs(int ms) {
    taosMsleep(ms);
}

void freeArrayPtr(char **ptr) {
    if (ptr == NULL) {
        return;
    }

    for (int i = 0; ptr[i] != NULL; i++) {
        taosMemoryFree(ptr[i]);
    }
    taosMemoryFree(ptr);
}

void freePtr(void *ptr) {
    if (ptr != NULL) {
        taosMemoryFree(ptr);
    }
}

const char* bckErrStr(int code) {
    switch (code) {
        case TSDB_CODE_BCK_INVALID_PARAM:        return "invalid parameter";
        case TSDB_CODE_BCK_MALLOC_FAILED:        return "memory allocation failed";
        case TSDB_CODE_BCK_CREATE_THREAD_FAILED: return "create thread failed";
        case TSDB_CODE_BCK_CREATE_FILE_FAILED:   return "create file failed";
        case TSDB_CODE_BCK_NO_FIELDS:            return "query result has no fields";
        case TSDB_CODE_BCK_FETCH_FIELDS_FAILED:  return "fetch fields failed";
        case TSDB_CODE_BCK_COMPRESS_FAILED:      return "compress failed";
        case TSDB_CODE_BCK_WRITE_FILE_FAILED:    return "write file failed";
        case TSDB_CODE_BCK_CONN_POOL_EXHAUSTED:  return "connection pool exhausted";
        case TSDB_CODE_BCK_READ_FILE_FAILED:     return "read file failed";
        case TSDB_CODE_BCK_INVALID_FILE:         return "invalid backup file";
        case TSDB_CODE_BCK_STMT_FAILED:          return "stmt bind/execute failed";
        case TSDB_CODE_BCK_DECOMPRESS_FAILED:    return "decompress failed";
        case TSDB_CODE_BCK_OPEN_DIR_FAILED:      return "open directory failed";
        case TSDB_CODE_BCK_EXEC_SQL_FAILED:      return "execute sql failed";
        case TSDB_CODE_BCK_USER_CANCEL:          return "cancelled by user";
        case TSDB_CODE_BCK_SPEC_TABLE_NOT_FOUND: return "specified database/table not found";
        case TSDB_CODE_BCK_INVALID_COMBINATION:  return "invalid option combination";
        default:                                 return NULL;
    }
}

const char* bckErrMsg(int code) {
    const char *msg = bckErrStr(code);
    if (!msg) msg = tstrerror(code);
    return msg;
}

bool errorCodeCanRetry(int code) {
    switch (code) {
        // RPC / network layer errors
        case TSDB_CODE_RPC_NETWORK_ERROR:
        case TSDB_CODE_RPC_NETWORK_BUSY:
        case TSDB_CODE_RPC_TIMEOUT:
        case TSDB_CODE_RPC_BROKEN_LINK:
        // Raft / consensus layer: leader election or log restore in progress
        case TSDB_CODE_SYN_NOT_LEADER:
        case TSDB_CODE_SYN_RESTORING:
        case TSDB_CODE_SYN_TIMEOUT:
        // Vnode temporarily busy (compaction, split, etc.)
        case TSDB_CODE_VND_QUERY_BUSY:
            return true;
        default:
            return false;
    }
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
                      int64_t fileCount,
                      StorageFormat format,
                      char *fileName, 
                      int len) {

    char * outPath = argOutPath();
    const char * ext = (format == BINARY_TAOS) ? "dat" : "par";
    int dirIndex = fileCount/FOLDER_MAXFILE;

    switch (fileType) {
    case BACK_DIR_DB:
        snprintf(fileName, len, "%s/%s", outPath, dbName);
        break;
    case BACK_FILE_DBSQL:
        snprintf(fileName, len, "%s/%s/db.sql", outPath, dbName);
        break;
    case BACK_FILE_STBSQL:
        snprintf(fileName, len, "%s/%s/stb.sql", outPath, dbName);
        break;
    case BACK_FILE_STBCSV:
        snprintf(fileName, len, "%s/%s/%s.csv", outPath, dbName, stbName);
        break;
    case BACK_DIR_TAG:
        snprintf(fileName, len, "%s/%s/tags/", outPath, dbName);
        break;
    case BACK_FILE_TAG:
        snprintf(fileName, len, "%s/%s/tags/%s_data%d.%s", outPath, dbName, stbName, index, ext);
        break;
    case BACK_DIR_DATA:
        snprintf(fileName, len, "%s/%s/%s_data%d", outPath, dbName, stbName, dirIndex);
        break;
    case BACK_FILE_DATA:
        snprintf(fileName, len, "%s/%s/%s_data%d/%s.%s", outPath, dbName, stbName, dirIndex, tbName, ext);
        break;
    case BACK_FILE_NTBSQL:
        snprintf(fileName, len, "%s/%s/ntb.sql", outPath, dbName);
        break;
    case BACK_DIR_NTBDATA:
        snprintf(fileName, len, "%s/%s/_ntb_data%d", outPath, dbName, dirIndex);
        break;
    case BACK_FILE_VTBSQL:
        snprintf(fileName, len, "%s/%s/vtb.sql", outPath, dbName);
        break;
    case BACK_DIR_VTAG:
        snprintf(fileName, len, "%s/%s/vtags/", outPath, dbName);
        break;
    case BACK_FILE_VTAG:
        snprintf(fileName, len, "%s/%s/vtags/%s_data%d.%s", outPath, dbName, stbName, index, ext);
        break;
    case BACK_FILE_STREAMSQL:
        snprintf(fileName, len, "%s/%s/stream.sql", outPath, dbName);
        break;
    case BACK_FILE_TOPICSQL:
        snprintf(fileName, len, "%s/%s/topic.sql", outPath, dbName);
        break;
    default:
        return TSDB_CODE_INVALID_PARA;
    }
    
    return TSDB_CODE_SUCCESS;
}
