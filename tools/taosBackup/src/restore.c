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
    
#include "restore.h"
#include "restoreMeta.h"
#include "restoreData.h"

//
// -------------------------------------- UTIL -----------------------------------------
//



//
// ------------------- main ---------------------
//

//
// restore database: meta first, then data
//
static int restoreDatabase(const char *dbName) {
    int code = TSDB_CODE_FAILED;

    // meta: create db, create stb, create child tables with tags
    code = restoreDatabaseMeta(dbName);
    if (code != TSDB_CODE_SUCCESS || g_interrupted) {
        if (g_interrupted && code == TSDB_CODE_SUCCESS) code = TSDB_CODE_BCK_USER_CANCEL;
        if (g_interrupted) logInfo("restore database:%s cancelled by user", dbName);
        else logError("restore database:%s meta failed, code: 0x%08X", dbName, code);
        return code;
    }

    // data: read .dat files and write via STMT
    code = restoreDatabaseData(dbName);
    if (code != TSDB_CODE_SUCCESS) {
        logError("restore database:%s data failed, code: 0x%08X", dbName, code);
        return code;
    }

    return code;
}

//
// restore main function
//
//
// Scan backup directory for database subdirectories
//
static char** scanBackupDatabases(int *count) {
    *count = 0;
    char *outPath = argOutPath();

    TdDirPtr dir = taosOpenDir(outPath);
    if (dir == NULL) {
        logError("open backup dir failed: %s", outPath);
        return NULL;
    }

    int capacity = 16;
    char **names = (char **)taosMemoryCalloc(capacity + 1, sizeof(char *));
    if (!names) {
        taosCloseDir(&dir);
        return NULL;
    }

    TdDirEntryPtr entry;
    while ((entry = taosReadDir(dir)) != NULL) {
        char *entryName = taosGetDirEntryName(entry);
        if (entryName[0] == '.') continue;

        // check if it's a directory containing db.sql
        char dbSqlPath[MAX_PATH_LEN];
        snprintf(dbSqlPath, sizeof(dbSqlPath), "%s/%s/db.sql", outPath, entryName);
        if (!taosCheckExistFile(dbSqlPath)) continue;

        if (*count >= capacity) {
            capacity *= 2;
            char **tmp = (char **)taosMemoryRealloc(names, (capacity + 1) * sizeof(char *));
            if (!tmp) {
                freeArrayPtr(names);
                taosCloseDir(&dir);
                return NULL;
            }
            names = tmp;
        }
        names[*count] = taosStrdup(entryName);
        (*count)++;
    }
    names[*count] = NULL;

    taosCloseDir(&dir);
    return names;
}

int restoreMain() {
    int code = TSDB_CODE_FAILED;

    // get backup databases to restore
    char **backDB = argBackDB();
    char **allDBs = NULL;

    if (backDB == NULL || backDB[0] == NULL) {
        // no -D specified: restore all databases found in backup directory
        int dbCount = 0;
        allDBs = scanBackupDatabases(&dbCount);
        if (allDBs == NULL || dbCount == 0) {
            logError("no database found in backup directory");
            if (allDBs) freeArrayPtr(allDBs);
            return TSDB_CODE_INVALID_PARA;
        }
        backDB = allDBs;
        logInfo("discovered %d database(s) to restore", dbCount);
    }

    // count total databases
    for (int i = 0; backDB[i] != NULL; i++) {
        g_stats.dbTotal++;
    }

    // loop restore each database (serial)
    for (int i = 0; backDB[i] != NULL; i++) {
        const char *targetDb = argRenameDb(backDB[i]);
        if (strcmp(targetDb, backDB[i]) != 0) {
            logInfo("restore database: %s -> %s", backDB[i], targetDb);
        } else {
            logInfo("restore database: %s", backDB[i]);
        }

        // restore: meta + data
        code = restoreDatabase(backDB[i]);
        if (code == TSDB_CODE_SUCCESS) {
            g_stats.dbSuccess++;
        } else {
            g_stats.dbFailed++;
            break;
        }
    }

    if (allDBs) freeArrayPtr(allDBs);
    return code;
}

