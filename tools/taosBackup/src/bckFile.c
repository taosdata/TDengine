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

int queryWriteTxt(const char *sql, const char *pathFile) {
    TAOS *conn = getConnection();
    if (!conn) {
        logError("get connection failed");
        return TSDB_CODE_FAILED;
    }

    TAOS_RES *res = taos_query(conn, sql);
    if (!res || taos_errno(res)) {
        logError("query failed: %s", taos_errstr(res));
        if (res) taos_free_result(res);
        releaseConnection(conn);
        return TSDB_CODE_FAILED;
    }

    FILE *fp = fopen(pathFile, "a");
    if (!fp) {
        logError("open file failed: %s", pathFile);
        taos_free_result(res);
        releaseConnection(conn);
        return TSDB_CODE_FAILED;
    }

    TAOS_ROW row;
    while ((row = taos_fetch_row(res))) {
        if (row[1]) {
            fprintf(fp, "%s;\n", (char *)row[1]);
        }
    }

    fclose(fp);
    taos_free_result(res);
    releaseConnection(conn);
    return TSDB_CODE_SUCCESS;
}


int queryWriteJson(const char *sql, const char *pathFile, char ** selectTags) {
    // get connection
    TAOS *conn = getConnection();
    if (!conn) {
        logError("get connection failed");
        return TSDB_CODE_FAILED;
    }

    // execute query
    TAOS_RES *res = taos_query(conn, sql);
    if (!res || taos_errno(res)) {
        logError("query failed: %s", taos_errstr(res));
        if (res) taos_free_result(res);
        releaseConnection(conn);
        return TSDB_CODE_FAILED;
    }

    // allocate tag buffer
    int tagCapacity = 256;
    int tagUsed = 0;
    char *tagBuf = NULL;
    if (selectTags && *selectTags == NULL) {
        tagBuf = (char *)calloc(tagCapacity, sizeof(char));
        if (!tagBuf) {
            logError("allocate tag buffer failed");
            taos_free_result(res);
            releaseConnection(conn);
            return TSDB_CODE_FAILED;
        }
    }

    // open file
    FILE *fp = fopen(pathFile, "w");
    if (!fp) {
        logError("open file failed: %s", pathFile);
        free(tagBuf);
        taos_free_result(res);
        releaseConnection(conn);
        return TSDB_CODE_FAILED;
    }

    // write json header
    fprintf(fp, "{\n  \"fields\": [\n");

    // fetch and write
    TAOS_ROW row;
    int first = 1;
    int firstTag = 1;
    while ((row = taos_fetch_row(res))) {
        if (!first) {
            fprintf(fp, ",\n");
        }
        first = 0;

        // extract fields
        const char *name = row[0] ? (char *)row[0] : "";
        const char *type = row[1] ? (char *)row[1] : "";
        int length = row[2] ? atoi((char *)row[2]) : 0;
        const char *note = row[3] ? (char *)row[3] : "";
        int isTag = (strstr(note, "TAG") != NULL) ? 1 : 0;

        // collect tag names
        if (isTag && tagBuf) {
            int nameLen = strlen(name);
            int needed = tagUsed + nameLen + (firstTag ? 0 : 1) + 1;
            
            while (needed > tagCapacity) {
                tagCapacity *= 2;
                char *newBuf = (char *)realloc(tagBuf, tagCapacity);
                if (!newBuf) {
                    logError("realloc tag buffer failed");
                    free(tagBuf);
                    fclose(fp);
                    taos_free_result(res);
                    releaseConnection(conn);
                    return TSDB_CODE_FAILED;
                }
                tagBuf = newBuf;
            }
            
            if (!firstTag) {
                tagBuf[tagUsed++] = ',';
            }
            strcpy(tagBuf + tagUsed, name);
            tagUsed += nameLen;
            firstTag = 0;
        }

        // write json object
        fprintf(fp, "    {\"name\": \"%s\", \"type\": \"%s\", \"length\": %d, \"tag\": %d}",
                name, type, length, isTag);
    }

    // write json footer
    fprintf(fp, "\n  ]\n}\n");

    fclose(fp);
    taos_free_result(res);
    releaseConnection(conn);
    
    if (selectTags) {
        *selectTags = tagBuf;
    } else {
        free(tagBuf);
    }
    
    return TSDB_CODE_SUCCESS;
}


// query result write to file with columnar storage
int queryWriteBinary(TAOS* conn, const char *sql, StorageFormat format, const char *pathFile) {
    int code = TSDB_CODE_FAILED;
    TAOS_RES* res = taos_query(conn, sql);
    if (res == NULL) {
        logError("query failed(%s): %s", taos_errstr(res), sql);
        return taos_errno(res);
    }

    if (format == BINARY_PARQUET) {
        // parquet
        code = resultToFileParquet(res, pathFile);
    } else {
        // taos
        code = resultToFileTaos(res, pathFile);
    }

    // free
    taos_free_result(res);    
    return code;
}
