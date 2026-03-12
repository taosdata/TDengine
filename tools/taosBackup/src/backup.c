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
    
#include "backup.h"
#include "backupMeta.h"
#include "backupData.h"
#include "bckArgs.h"
#include "bckPool.h"
#include "bckProgress.h"

//
// -------------------------------------- UTIL -----------------------------------------
//



//
// ------------------- main ---------------------
//

//
// backup database
//
int backDatabase(const char *dbName) {
    int code = TSDB_CODE_FAILED;
    DBInfo dbInfo;
    dbInfo.dbName = dbName;

    // create db main dir
    char path[MAX_PATH_LEN];
    obtainFileName(BACK_DIR_DB, dbName, NULL, NULL, 0, 0, argStorageFormat(), path, sizeof(path));
    code = taosMulMkDir(path);
    if (code != TSDB_CODE_SUCCESS) {
        logError("create db main path failed: %s", path);
        return code;
    }
    // tag dir
    obtainFileName(BACK_DIR_TAG, dbName, NULL, NULL, 0, 0, argStorageFormat(), path, sizeof(path));
    code = taosMkDir(path);
    if (code != TSDB_CODE_SUCCESS) {
        logError("create db tag path failed: %s", path);
        return code;
    }

    // meta
    code = backDatabaseMeta(&dbInfo);
    if (code != TSDB_CODE_SUCCESS) {
        if (code != TSDB_CODE_BCK_USER_CANCEL) {
            logError("backup database: %s meta failed, code: 0x%08X", dbName, code);
        }
        return code;
    }

    // data
    if (!argSchemaOnly()) {
        g_progress.phase = PROGRESS_PHASE_DATA;
        code = backDatabaseData(&dbInfo);
        if (code != TSDB_CODE_SUCCESS) {
            if (code != TSDB_CODE_BCK_USER_CANCEL) {
                logError("backup database: %s data failed, code: 0x%08X", dbName, code);
            }
            return code;
        }
    } else {
        logInfo("schema-only mode, skip data backup for database: %s", dbName);
    }

    return code;
}

//
// backup main function
//
//
// Query all non-system databases from TDengine
//
static char** getAllDatabases(int *count, int *retCode) {
    *count = 0;
    *retCode = TSDB_CODE_SUCCESS;
    logInfo("Connect to %s:%d ...", argHost(), argPort());
    TAOS *conn = getConnection(retCode);
    if (!conn) {
        return NULL;
    }

    TAOS_RES *res = taos_query(conn, "SHOW DATABASES;");
    *retCode = taos_errno(res);
    if (!res || *retCode) {
        logError("query databases failed(0x%08X %s): SHOW DATABASES", *retCode, taos_errstr(res));
        if (res) taos_free_result(res);
        releaseConnection(conn);
        return NULL;
    }

    int capacity = 16;
    char **names = (char **)taosMemoryCalloc(capacity + 1, sizeof(char *));
    if (!names) {
        *retCode = TSDB_CODE_BCK_MALLOC_FAILED;
        taos_free_result(res);
        releaseConnection(conn);
        return NULL;
    }

    TAOS_ROW row;
    while ((row = taos_fetch_row(res))) {
        int32_t *lens = taos_fetch_lengths(res);
        char dbName[TSDB_DB_NAME_LEN] = {0};
        int len = lens[0] < TSDB_DB_NAME_LEN - 1 ? lens[0] : TSDB_DB_NAME_LEN - 1;
        memcpy(dbName, row[0], len);
        dbName[len] = '\0';

        // skip system databases
        if (strcmp(dbName, "information_schema") == 0 ||
            strcmp(dbName, "performance_schema") == 0) {
            continue;
        }

        if (*count >= capacity) {
            capacity *= 2;
            char **tmp = (char **)taosMemoryRealloc(names, (capacity + 1) * sizeof(char *));
            if (!tmp) {
                freeArrayPtr(names);
                *retCode = TSDB_CODE_BCK_MALLOC_FAILED;
                taos_free_result(res);
                releaseConnection(conn);
                return NULL;
            }
            names = tmp;
        }
        names[*count] = taosStrdup(dbName);
        (*count)++;
    }
    names[*count] = NULL;

    taos_free_result(res);
    releaseConnection(conn);
    return names;
}

int backupMain() {
    int code = TSDB_CODE_FAILED;

    // get backup databases
    char **backDB = argBackDB();
    char **allDBs = NULL;

    if (backDB == NULL || backDB[0] == NULL) {
        // no -D specified: backup all non-system databases
        int dbCount = 0;
        allDBs = getAllDatabases(&dbCount, &code);
        if (allDBs == NULL || dbCount == 0) {
            if (allDBs) freeArrayPtr(allDBs);
            if (code != TSDB_CODE_SUCCESS) return code;
            logError("no database found to backup");
            return TSDB_CODE_INVALID_PARA;
        }
        backDB = allDBs;
        logInfo("discovered %d database(s) to backup", dbCount);
    }

    // count total databases
    for (int i = 0; backDB[i] != NULL; i++) {
        g_stats.dbTotal++;
    }
    g_progress.dbTotal = g_stats.dbTotal;

    // loop backup each database
    for (int i = 0; backDB[i] != NULL; i++) {
        g_progress.dbIndex = i + 1;
        snprintf(g_progress.dbName, sizeof(g_progress.dbName), "%s", backDB[i]);

        // backup data
        code = backDatabase(backDB[i]);
        if (code == TSDB_CODE_SUCCESS) {
            g_stats.dbSuccess++;
            logInfo("[%d/%d] database backup completed: %s", i+1, (int)g_stats.dbTotal, backDB[i]);
        } else {
            g_stats.dbFailed++;
            break;
        }
    }

    if (allDBs) freeArrayPtr(allDBs);
    return code;
}
