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
#include "bckProgress.h"

#ifdef COMPAT_AVRO_ENABLED
#include "compatAvro.h"
#endif

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

#ifdef COMPAT_AVRO_ENABLED
    // Check if this is a taosdump AVRO backup directory
    char avroDbPath[MAX_PATH_LEN];
    snprintf(avroDbPath, sizeof(avroDbPath), "%s/%s", argOutPath(), dbName);
    if (isAvroBackupDir(avroDbPath)) {
        logInfo("detected taosdump AVRO format for db: %s", dbName);
        code = restoreAvroDatabase(avroDbPath);
        if (code != 0) {
            logError("AVRO restore failed for db: %s, code: 0x%08X", dbName, code);
        }
        return code;
    }
#endif

    // meta: create db, create stb, create child tables with tags
    code = restoreDatabaseMeta(dbName);
    if (code != TSDB_CODE_SUCCESS || g_interrupted) {
        if (g_interrupted && code == TSDB_CODE_SUCCESS) code = TSDB_CODE_BCK_USER_CANCEL;
        if (g_interrupted) logInfo("restore database: %s cancelled by user", dbName);
        else logError("restore database: %s meta failed, code: 0x%08X", dbName, code);
        return code;
    }

    // data: read .dat files and write via STMT
    g_progress.phase = PROGRESS_PHASE_DATA;
    code = restoreDatabaseData(dbName);
    if (code != TSDB_CODE_SUCCESS) {
        logError("restore database: %s data failed, code: 0x%08X", dbName, code);
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

        // check if it's a directory containing db.sql (taosBackup) or dbs.sql (taosdump AVRO)
        char dbSqlPath[MAX_PATH_LEN];
        snprintf(dbSqlPath, sizeof(dbSqlPath), "%s/%s/db.sql", outPath, entryName);
        bool hasDbSql = taosCheckExistFile(dbSqlPath);

#ifdef COMPAT_AVRO_ENABLED
        if (!hasDbSql) {
            snprintf(dbSqlPath, sizeof(dbSqlPath), "%s/%s/dbs.sql", outPath, entryName);
            hasDbSql = taosCheckExistFile(dbSqlPath);
        }
#endif

        if (!hasDbSql) continue;

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

#ifdef COMPAT_AVRO_ENABLED
    // Check if outpath itself is a taosdump AVRO directory (single-db export)
    char *outPath0 = argOutPath();
    if (isAvroBackupDir(outPath0)) {
        logInfo("detected taosdump AVRO format at top-level: %s", outPath0);
        code = restoreAvroDatabase(outPath0);
        return code;
    }
#endif

    // get backup databases to restore
    char **backDB = argBackDB();
    char **allDBs = NULL;

    if (backDB == NULL || backDB[0] == NULL) {
        // no -D specified: restore all databases found in backup directory
        int dbCount = 0;
        allDBs = scanBackupDatabases(&dbCount);
        if (allDBs == NULL || dbCount == 0) {
            if (g_interrupted) {
                if (allDBs) freeArrayPtr(allDBs);
                return TSDB_CODE_BCK_USER_CANCEL;
            }
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
    g_progress.dbTotal = g_stats.dbTotal;

    // loop restore each database (serial)
    for (int i = 0; backDB[i] != NULL; i++) {
        g_progress.dbIndex = i + 1;
        snprintf(g_progress.dbName, sizeof(g_progress.dbName), "%s", backDB[i]);
        // reset per-DB progress fields for each database
        g_progress.stbTotal = 0;
        g_progress.stbIndex = 0;
        g_progress.stbName[0] = '\0';
        atomic_store_64(&g_progress.ctbTotalAll, 0);
        atomic_store_64(&g_progress.ctbDoneAll, 0);
        atomic_store_64(&g_progress.ctbDoneCur, 0);
        g_progress.ctbTotalCur = 0;
        const char *targetDb = argRenameDb(backDB[i]);
        if (strcmp(targetDb, backDB[i]) != 0) {
            logInfo("[%d/%d] db: %s -> %s  restore start",
                    i + 1, (int)g_stats.dbTotal, backDB[i], targetDb);
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

