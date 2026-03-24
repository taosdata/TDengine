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
    
#include "bck.h"
#include "bckFile.h"
#include "bckPool.h"
#include "storageTaos.h"
#include "storageParquet.h"

int queryWriteTxt(const char *sql, int32_t col, const char *pathFile) {
    int connCode = TSDB_CODE_FAILED;
    TAOS *conn = getConnection(&connCode);
    if (!conn) {
        logError("get connection failed");
        return connCode;
    }

    TAOS_RES *res = taos_query(conn, sql);
    int32_t code = taos_errno(res);
    if (!res || code) {
        logError("query failed(0x%08X %s): %s", code, taos_errstr(res), sql);
        if (res) taos_free_result(res);
        releaseConnection(conn);
        return code;
    }

    int32_t num_fields = taos_num_fields(res);
    if (col < 0 || col >= num_fields) {
        logError("invalid column index: %d >= num_fields: %d", col, num_fields);
        taos_free_result(res);
        releaseConnection(conn);
        return TSDB_CODE_BCK_INVALID_PARAM;
    }

    TdFilePtr fp = taosOpenFile(pathFile,  TD_FILE_WRITE | TD_FILE_CREATE);
    if (!fp) {
        logError("open file failed: %s", pathFile);
        taos_free_result(res);
        releaseConnection(conn);
        return TSDB_CODE_BCK_CREATE_FILE_FAILED;
    }

    TAOS_ROW row;
    code = TSDB_CODE_SUCCESS;
    while ((row = taos_fetch_row(res))) {
        if (row[col]) {
            int32_t *length = taos_fetch_lengths(res);
            int64_t written = taosWriteFile(fp, row[col], length[col]);
            if (written != length[col]) {
                logError("write file failed: %s len=%"PRId64 " written=%"PRId64, pathFile, length[col], written);
                code = TSDB_CODE_BCK_WRITE_FILE_FAILED;
                break;
            }
            taosWriteFile(fp, "\n", 1);
        } else {
            code = TSDB_CODE_BCK_NO_FIELDS;
            logError("no fields in query result: %s", sql);
        }
    }

    taosCloseFile(&fp);
    taos_free_result(res);
    releaseConnection(conn);
    return code;
}


int queryWriteCsv(const char *sql, const char *pathFile, char ** selectTags) {
    int connCode = TSDB_CODE_FAILED;
    TAOS *conn = getConnection(&connCode);
    if (!conn) {
        logError("get connection failed");
        return connCode;
    }

    TAOS_RES *res = taos_query(conn, sql);
    if (!res || taos_errno(res)) {
        logError("query failed: %s", taos_errstr(res));
        if (res) taos_free_result(res);
        releaseConnection(conn);
        return TSDB_CODE_FAILED;
    }

    TdFilePtr fp = taosOpenFile(pathFile, TD_FILE_WRITE | TD_FILE_CREATE);
    if (!fp) {
        logError("open file failed: %s", pathFile);
        taos_free_result(res);
        releaseConnection(conn);
        return TSDB_CODE_BCK_CREATE_FILE_FAILED;
    }

    // Write CSV header
    const char* header = "field,type,length,note\n";
    int64_t headerLen = strlen(header);
    if (taosWriteFile(fp, header, headerLen) != headerLen) {
        logError("write CSV header failed: %s", pathFile);
        taosCloseFile(&fp);
        taos_free_result(res);
        releaseConnection(conn);
        return TSDB_CODE_BCK_WRITE_FILE_FAILED;
    }

    // Dynamic buffer to collect tag fields
    int32_t tagBufferSize = 256;  // Initial size
    char* tagBuffer = taosMemoryMalloc(tagBufferSize);
    if (!tagBuffer) {
        logError("memory allocation failed for tagBuffer");
        taosCloseFile(&fp);
        taos_free_result(res);
        releaseConnection(conn);
        return TSDB_CODE_OUT_OF_MEMORY;
    }
    tagBuffer[0] = '\0';
    bool firstTag = true;
    
    TAOS_ROW row;
    int32_t code = TSDB_CODE_SUCCESS;
    while ((row = taos_fetch_row(res))) {
        // Expected columns: field, type, length, note
        if (!row[0] || !row[1] || !row[2]) {
            logError("incomplete row data in query result");
            code = TSDB_CODE_BCK_NO_FIELDS;
            break;
        }
        int32_t *lens = taos_fetch_lengths(res);

        char* field = row[0];
        char* type = row[1];
        int32_t* length = (int32_t*)row[2];
        char* note = row[3] ? row[3] : "";
        
        // Write CSV row: field,type,length,note
        char csvRow[512];
        int ret = snprintf(csvRow, sizeof(csvRow), "%.*s,%.*s,%d,%.*s\n", lens[0], field, lens[1], type, *length, lens[3], note);
        if (ret >= sizeof(csvRow)) {
            logError("CSV row too long for buffer");
            code = TSDB_CODE_BCK_WRITE_FILE_FAILED;
            break;
        }
        
        int64_t rowLen = strlen(csvRow);
        if (taosWriteFile(fp, csvRow, rowLen) != rowLen) {
            logError("write CSV row failed: %s", pathFile);
            code = TSDB_CODE_BCK_WRITE_FILE_FAILED;
            break;
        }

        // Collect TAG fields
        if (note && lens[3] >= 3 && strncmp(note, "TAG", 3) == 0) {
            int32_t currentLen = strlen(tagBuffer);
            int32_t fieldLen = lens[0];
            // +1 for comma, +1 for \0 +2 for ``
            int32_t needLen = currentLen + (firstTag ? 0 : 1) + fieldLen + 1 + 2; 
            
            // Check if we need to expand the buffer
            if (needLen >= tagBufferSize) {
                while (needLen >= tagBufferSize) {
                    tagBufferSize *= 2;  // Double the size
                }
                char* newBuffer = taosMemoryRealloc(tagBuffer, tagBufferSize);
                if (!newBuffer) {
                    logError("memory reallocation failed for tagBuffer");
                    code = TSDB_CODE_OUT_OF_MEMORY;
                    break;
                }
                tagBuffer = newBuffer;
            }
            
            if (firstTag) {
                snprintf(tagBuffer, tagBufferSize, "`%.*s`", fieldLen, field);
                firstTag = false;
            } else {
                int32_t cur = strlen(tagBuffer);
                snprintf(tagBuffer + cur, tagBufferSize - cur, ",`%.*s`", fieldLen, field);
            }
        }
    }

    // Set selectTags if provided
    if (selectTags) {
        if (strlen(tagBuffer) > 0) {
            // Directly assign tagBuffer to selectTags to avoid extra allocation and copy
            *selectTags = tagBuffer;
        } else {
            // No tags found, free the buffer and set to NULL
            taosMemoryFree(tagBuffer);
            *selectTags = NULL;
        }
    } else {
        // selectTags is NULL, free the buffer
        taosMemoryFree(tagBuffer);
    }
    
    taosCloseFile(&fp);
    taos_free_result(res);
    releaseConnection(conn);
    return code;
}


// query result write to file with columnar storage
int queryWriteBinary(TAOS* conn, const char *sql, StorageFormat format, const char *pathFile, int64_t *outRows) {
    int code = TSDB_CODE_FAILED;
    TAOS_RES* res = taos_query(conn, sql);
    if (res == NULL) {
        logError("query failed(%s): %s", taos_errstr(res), sql);
        return taos_errno(res);
    }
    logDebug("sql result to file: %s -> %s", sql, pathFile);

    int64_t rows = 0;
    if (format == BINARY_PARQUET) {
        // parquet
        code = resultToFileParquet(res, pathFile, &rows);
    } else {
        // taos (writeBuf=NULL: will allocate internally for now)
        code = resultToFileTaos(res, pathFile, NULL, 0, &rows);
    }

    if (outRows) *outRows = rows;

    // free
    taos_free_result(res);
    return code;
}

int queryWriteBinaryEx(TAOS* conn, const char *sql, StorageFormat format, const char *pathFile,
                       char *writeBuf, int32_t writeBufCap, int64_t *outRows) {
    int code = TSDB_CODE_FAILED;
    TAOS_RES* res = taos_query(conn, sql);
    if (res == NULL) {
        logError("query failed(%s): %s", taos_errstr(res), sql);
        return taos_errno(res);
    }
    logDebug("sql result to file: %s -> %s", sql, pathFile);

    int64_t rows = 0;
    if (format == BINARY_PARQUET) {
        code = resultToFileParquet(res, pathFile, &rows);
    } else {
        code = resultToFileTaos(res, pathFile, writeBuf, writeBufCap, &rows);
    }

    if (outRows) *outRows = rows;
    taos_free_result(res);
    return code;
}
