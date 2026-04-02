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

char ** getDBSuperTableNames(const char *dbName, int *code) {
    *code = TSDB_CODE_FAILED;
    
    // get connection
    TAOS *conn = getConnection(code);
    if (!conn) {
        logError("get connection failed");
        return NULL;
    }

    // query stables
    char sql[256];
    snprintf(sql, sizeof(sql), "SHOW `%s`.STABLES", dbName);
    TAOS_RES *res = taos_query(conn, sql);
    *code = taos_errno(res);
    if (!res || *code) {
        logError("query stables failed(0x%08X %s): %s", *code, taos_errstr(res), sql);
        if (res) taos_free_result(res);
        releaseConnection(conn);
        return NULL;
    }

    // count rows
    int count = 0;
    int capacity = 16;
    char **names = (char **)taosMemoryCalloc(capacity + 1, sizeof(char *));
    if (!names) {
        *code = TSDB_CODE_BCK_MALLOC_FAILED;
        taos_free_result(res);
        releaseConnection(conn);
        return NULL;
    }

    // fetch rows
    TAOS_ROW row;
    while ((row = taos_fetch_row(res))) {
        int32_t *length = taos_fetch_lengths(res);
        if (count >= capacity) {
            capacity *= 2;
            char **tmp = (char **)taosMemoryRealloc(names, (capacity + 1) * sizeof(char *));
            if (!tmp) {
                freeArrayPtr(names);
                *code = TSDB_CODE_BCK_MALLOC_FAILED;
                taos_free_result(res);
                releaseConnection(conn);
                return NULL;
            }
            names = tmp;
        }
        names[count++] = tstrndup((char *)row[0], length[0]);
    }
    names[count] = NULL;

    taos_free_result(res);
    releaseConnection(conn);
    *code = TSDB_CODE_SUCCESS;
    return names;
}

char ** getDBNormalTableNames(const char *dbName, int *code) {
    *code = TSDB_CODE_FAILED;
    
    TAOS *conn = getConnection(code);
    if (!conn) {
        logError("get connection failed");
        return NULL;
    }

    char sql[1024];
    char specFilter[512] = "";
    if (argSpecTables()) {
        char inClause[400] = "";
        argBuildInClause("table_name", inClause, sizeof(inClause));
        snprintf(specFilter, sizeof(specFilter), " AND %s", inClause);
    }
    snprintf(sql, sizeof(sql),
             "SELECT table_name FROM information_schema.ins_tables "
             "WHERE db_name='%s' AND stable_name IS NULL"
             " AND type NOT LIKE 'VIRTUAL%%'%s ORDER BY table_name", dbName, specFilter);
    TAOS_RES *res = taos_query(conn, sql);
    *code = taos_errno(res);
    if (!res || *code) {
        logError("query normal tables failed(0x%08X %s): %s", *code, taos_errstr(res), sql);
        if (res) taos_free_result(res);
        releaseConnection(conn);
        return NULL;
    }

    int count = 0;
    int capacity = 16;
    char **names = (char **)taosMemoryCalloc(capacity + 1, sizeof(char *));
    if (!names) {
        *code = TSDB_CODE_BCK_MALLOC_FAILED;
        taos_free_result(res);
        releaseConnection(conn);
        return NULL;
    }

    TAOS_ROW row;
    while ((row = taos_fetch_row(res))) {
        int32_t *length = taos_fetch_lengths(res);
        if (count >= capacity) {
            capacity *= 2;
            char **tmp = (char **)taosMemoryRealloc(names, (capacity + 1) * sizeof(char *));
            if (!tmp) {
                freeArrayPtr(names);
                *code = TSDB_CODE_BCK_MALLOC_FAILED;
                taos_free_result(res);
                releaseConnection(conn);
                return NULL;
            }
            names = tmp;
        }
        names[count++] = tstrndup((char *)row[0], length[0]);
    }
    names[count] = NULL;

    taos_free_result(res);
    releaseConnection(conn);
    *code = TSDB_CODE_SUCCESS;
    return names;
}

char ** getDBVirtualTableNames(const char *dbName, int *code) {
    *code = TSDB_CODE_FAILED;

    TAOS *conn = getConnection(code);
    if (!conn) {
        logError("get connection failed");
        return NULL;
    }

    char sql[512];
    snprintf(sql, sizeof(sql),
             "SELECT table_name FROM information_schema.ins_tables "
             "WHERE db_name='%s' AND (type='VIRTUAL_NORMAL_TABLE' OR type='VIRTUAL_CHILD_TABLE') "
             "ORDER BY table_name", dbName);
    TAOS_RES *res = taos_query(conn, sql);
    if (!res || taos_errno(res)) {
        *code = taos_errno(res);
        logError("query virtual tables failed: %s", taos_errstr(res));
        if (res) taos_free_result(res);
        releaseConnection(conn);
        return NULL;
    }

    int count = 0;
    int capacity = 16;
    char **names = (char **)taosMemoryCalloc(capacity + 1, sizeof(char *));
    if (!names) {
        *code = TSDB_CODE_BCK_MALLOC_FAILED;
        taos_free_result(res);
        releaseConnection(conn);
        return NULL;
    }

    TAOS_ROW row;
    while ((row = taos_fetch_row(res))) {
        int32_t *length = taos_fetch_lengths(res);
        if (count >= capacity) {
            capacity *= 2;
            char **tmp = (char **)taosMemoryRealloc(names, (capacity + 1) * sizeof(char *));
            if (!tmp) {
                freeArrayPtr(names);
                *code = TSDB_CODE_BCK_MALLOC_FAILED;
                taos_free_result(res);
                releaseConnection(conn);
                return NULL;
            }
            names = tmp;
        }
        names[count++] = tstrndup((char *)row[0], length[0]);
    }
    names[count] = NULL;

    taos_free_result(res);
    releaseConnection(conn);
    *code = TSDB_CODE_SUCCESS;
    return names;
}

int getDBNormalTableCount(const char *dbName, int32_t *outCount) {
    char specFilter[512] = "";
    if (argSpecTables()) {
        char inClause[400] = "";
        argBuildInClause("table_name", inClause, sizeof(inClause));
        snprintf(specFilter, sizeof(specFilter), " AND %s", inClause);
    }
    char sql[1024];
    snprintf(sql, sizeof(sql), 
             "SELECT count(*) FROM information_schema.ins_tables "
             "WHERE db_name='%s' AND stable_name IS NULL"
             " AND type NOT LIKE 'VIRTUAL%%'%s", dbName, specFilter);
    return queryValueInt(sql, 0, outCount);
}

int queryValueInt(const char *sql, int col, int32_t *outValue) {
    int connCode = TSDB_CODE_FAILED;
    TAOS* conn = getConnection(&connCode);
    if (conn == NULL) {
        return connCode;
    }
    TAOS_RES *res = taos_query(conn, sql);
    int code = taos_errno(res);
    if (res == NULL) {
        releaseConnection(conn);
        return code;
    }
    TAOS_ROW row = taos_fetch_row(res);
    if (row) {
        *outValue = *(int32_t*)row[col];
    } else {
        code = taos_errno(res);
    }
    taos_free_result(res);
    releaseConnection(conn);
    return code;
}