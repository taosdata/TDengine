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
#include "bckUtil.h"

#ifdef COMPAT_AVRO_ENABLED
#include "compatAvro.h"
#endif

//
// -------------------------------------- UTIL -----------------------------------------
//



//
// ------------------- main ---------------------
//

#ifdef COMPAT_AVRO_ENABLED
// Build the per-database path and report whether it holds an old taosdump
// AVRO-format backup, which is restored by a self-contained code path.
static bool isAvroDb(const char *dbName, char *outPath, int outLen) {
    snprintf(outPath, outLen, "%s/%s", argOutPath(), dbName);
    return isAvroBackupDir(outPath);
}
#endif

//
// restore one database's basic content: meta (schemas + tags) first, then data
//
static int restoreDatabaseBasic(const char *dbName) {
    int code = TSDB_CODE_FAILED;

#ifdef COMPAT_AVRO_ENABLED
    // AVRO backups carry their own layout and restore order (including a second
    // pass for virtual tables), so the whole database is handled here.
    char avroDbPath[MAX_PATH_LEN];
    if (isAvroDb(dbName, avroDbPath, sizeof(avroDbPath))) {
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
// restore one database's extended-metadata "shell": the database itself and its
// virtual super tables.  Used only by --content=ext-meta (the basic stage never
// ran, so no database exists yet).
//
static int restoreDatabaseExtMetaPrepareOne(const char *dbName) {
#ifdef COMPAT_AVRO_ENABLED
    // The basic stage (which does the AVRO restore) never ran for this db, so
    // there is nothing this stage can restore for an AVRO backup in that mode.
    char avroDbPath[MAX_PATH_LEN];
    if (isAvroDb(dbName, avroDbPath, sizeof(avroDbPath))) {
        logError("db %s is a taosdump AVRO backup: --content=ext-meta cannot "
                  "restore it standalone, run without --content or with basic+ext-meta", dbName);
        return TSDB_CODE_BCK_INVALID_COMBINATION;
    }
#endif

    return restoreDatabaseExtMetaPrepare(dbName);
}

//
// restore one database's extended metadata DDL: virtual tables, streams, topics
//
static int restoreDatabaseExtMetaApplyOne(const char *dbName) {
#ifdef COMPAT_AVRO_ENABLED
    // AVRO backups have no vtb.sql / stream.sql / topic.sql; their virtual
    // tables were already handled inside restoreAvroDatabase() during stage 1.
    char avroDbPath[MAX_PATH_LEN];
    if (isAvroDb(dbName, avroDbPath, sizeof(avroDbPath))) {
        logWarn("db %s is a taosdump AVRO backup, ext meta stage skipped", dbName);
        return TSDB_CODE_SUCCESS;
    }
#endif

    return restoreDatabaseExtMetaApply(dbName);
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

    // Validate user-specified table names (positional args) exist in the backup
    // files before starting any restore work.
    if (argSpecTables()) {
        for (int i = 0; backDB[i] != NULL; i++) {
            code = validateSpecTablesForRestore(backDB[i]);
            if (code != TSDB_CODE_SUCCESS) {
                logError("spec-table validation failed for database '%s'", backDB[i]);
                if (allDBs) freeArrayPtr(allDBs);
                return code;
            }
        }
    }

    code = TSDB_CODE_SUCCESS;

    //
    // Stage 1: basic content (physical tables + data) for ALL databases.
    //
    // This must finish across every database before any extended metadata runs,
    // because a virtual table in one database can reference tables in another.
    //
    if (argContentBasic()) {
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
            code = restoreDatabaseBasic(backDB[i]);
            if (code == TSDB_CODE_SUCCESS) {
                g_stats.dbSuccess++;
            } else {
                g_stats.dbFailed++;
                break;
            }
        }
    }

    //
    // Stage 2: extended metadata (virtual tables, streams, topics) for ALL databases.
    //
    // Every database's physical tables now exist, so cross-database references
    // resolve regardless of the order the databases are listed in.
    //
    if (argContentExtMeta() && code == TSDB_CODE_SUCCESS && !g_interrupted) {
        // With --content=ext-meta the basic stage never ran, so each database
        // must be created here and counted here (stage 1 does the counting
        // otherwise, and double-counting would corrupt the end summary).
        bool extMetaOnly = !argContentBasic();

        // Create EVERY database (and its virtual super tables) before applying
        // ANY extended-metadata DDL.  A stream's INTO target — like a virtual
        // table's source columns — can live in a different database, and the
        // restore order here comes from raw readdir(), which is filesystem-
        // dependent.  Without this two-pass split, a cross-database stream
        // fails with "Database not exist" whenever the referencing database is
        // restored before the referenced one.
        if (extMetaOnly) {
            for (int i = 0; backDB[i] != NULL && code == TSDB_CODE_SUCCESS; i++) {
                logInfo("[%d/%d] db: %s  ext meta prepare start",
                        i + 1, (int)g_stats.dbTotal, backDB[i]);
                code = restoreDatabaseExtMetaPrepareOne(backDB[i]);
                if (code != TSDB_CODE_SUCCESS) {
                    g_stats.dbFailed++;
                }
            }
        }

        for (int i = 0; backDB[i] != NULL && code == TSDB_CODE_SUCCESS; i++) {
            g_progress.dbIndex = i + 1;
            snprintf(g_progress.dbName, sizeof(g_progress.dbName), "%s", backDB[i]);
            g_progress.stbTotal   = 0;
            g_progress.stbIndex   = 0;
            g_progress.stbName[0] = '\0';
            // Clear the CTB counters left over from the previous database (or
            // stage 1).  The progress thread goes quiet once ctbDoneCur >=
            // ctbTotalCur, so stale "complete" counters would suppress the
            // whole ext-meta phase display for this database.
            atomic_store_64(&g_progress.ctbTotalAll, 0);
            atomic_store_64(&g_progress.ctbDoneAll, 0);
            atomic_store_64(&g_progress.ctbDoneCur, 0);
            g_progress.ctbTotalCur = 0;

            logInfo("[%d/%d] db: %s  ext meta restore start",
                    i + 1, (int)g_stats.dbTotal, backDB[i]);

            code = restoreDatabaseExtMetaApplyOne(backDB[i]);
            if (code == TSDB_CODE_SUCCESS) {
                if (extMetaOnly) g_stats.dbSuccess++;
            } else {
                if (extMetaOnly) {
                    g_stats.dbFailed++;
                } else {
                    // stage 1 already counted this db as dbSuccess; reverse
                    // that now that stage 2 failed for it.
                    g_stats.dbSuccess--;
                    g_stats.dbFailed++;
                }
            }
        }
    }

    if (allDBs) freeArrayPtr(allDBs);
    return code;
}

