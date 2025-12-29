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
    
#include "bckArgs.h"
#include "bckLog.h"
#include "bckDb.h"
#include <stdio.h>

char ** getDBSuperTableNames(const char *dbName, int *code) {
    *code = TSDB_CODE_FAILED;
    
    // get connection
    TAOS *conn = getConnection();
    if (!conn) {
        logError("get connection failed");
        return NULL;
    }

    // query stables
    char sql[256];
    snprintf(sql, sizeof(sql), "SHOW %s.STABLES", dbName);
    TAOS_RES *res = taos_query(conn, sql);
    if (!res || taos_errno(res)) {
        logError("query stables failed: %s", taos_errstr(res));
        if (res) taos_free_result(res);
        releaseConnection(conn);
        return NULL;
    }

    // count rows
    int count = 0;
    int capacity = 16;
    char **names = (char **)calloc(capacity + 1, sizeof(char *));
    if (!names) {
        taos_free_result(res);
        releaseConnection(conn);
        return NULL;
    }

    // fetch rows
    TAOS_ROW row;
    while ((row = taos_fetch_row(res))) {
        if (count >= capacity) {
            capacity *= 2;
            char **tmp = (char **)realloc(names, (capacity + 1) * sizeof(char *));
            if (!tmp) {
                freeArrayPtr(names);
                taos_free_result(res);
                releaseConnection(conn);
                return NULL;
            }
            names = tmp;
        }
        names[count++] = strdup((char *)row[0]);
    }
    names[count] = NULL;

    taos_free_result(res);
    releaseConnection(conn);
    *code = TSDB_CODE_SUCCESS;
    return names;
}