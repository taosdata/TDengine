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
    
#include "restoreData.h"
#include "restoreStmt.h"
#include "restoreStmt2.h"
#include "restoreCkpt.h"
#include "storageTaos.h"
#include "storageParquet.h"
#include "parquetBlock.h"
#include "colCompress.h"
#include "blockReader.h"
#include "bckPool.h"
#include "bckDb.h"
#include "bckSchemaChange.h"
#include "decimal.h"
#include "ttypes.h"
#include "osString.h"
#include "bckProgress.h"

// Forward declarations (temporary workaround)
extern bool stmt2ResetOnError(Stmt2RestoreCtx *ctx);
extern bool stmtResetOnError(StmtRestoreCtx *ctx);
extern void stmt2FreeSlot(Stmt2TableSlot *slot);
extern void freeBindArray(TAOS_MULTI_BIND *bindArray, int numFields);

static void* restoreDataThread(void *arg) {
    RestoreDataThread *thread = (RestoreDataThread *)arg;
    thread->code = TSDB_CODE_SUCCESS;

    const char *dbName = thread->dbInfo->dbName;
    const char *stbName4log = thread->stbInfo ? thread->stbInfo->stbName : "(ntb)";
    logInfo("data thread %d started for %s.%s (files: %d)",
            thread->index, dbName, stbName4log, thread->fileCnt);
    StmtVersion  stmtVer = argStmtVersion();

    /* ---- STMT2 path ---- */
    Stmt2RestoreCtx s2Ctx;
    memset(&s2Ctx, 0, sizeof(s2Ctx));
    s2Ctx.conn      = thread->conn;
    s2Ctx.dbName    = argRenameDb(dbName);
    s2Ctx.stbChange = thread->stbChange;

    /* Allocate pending-slots array for multi-table batching */
    s2Ctx.pendingSlots = (Stmt2TableSlot *)taosMemoryCalloc(
        STMT2_MULTI_TABLE_PENDING, sizeof(Stmt2TableSlot));
    if (!s2Ctx.pendingSlots) {
        logError("restore thread %d: alloc pendingSlots failed", thread->index);
        thread->code = TSDB_CODE_BCK_MALLOC_FAILED;
        return NULL;
    }

    /* Enable multi-table batching for native connections (not WebSocket/DSN).
     * WebSocket STMT2 prepare with INSERT INTO ? fails at the Rust driver level.
     * Set env TAOSBK_SINGLE_TABLE=1 to force single-table mode (benchmark only). */
    s2Ctx.multiTable = (argDriver() != CONN_MODE_WEBSOCKET && !argIsDsn() &&
                        getenv("TAOSBK_SINGLE_TABLE") == NULL);
    logDebug("restore thread %d: STMT2 multiTable=%d", thread->index, s2Ctx.multiTable);

    /* ---- STMT1 (legacy) path ---- */
    StmtRestoreCtx bCtx;
    memset(&bCtx, 0, sizeof(bCtx));
    if (stmtVer == STMT_VERSION_1) {
        TAOS_STMT *s1 = initStmt(thread->conn, true);
        if (!s1) {
            logError("restore thread %d: initStmt failed", thread->index);
            thread->code = TSDB_CODE_BCK_STMT_FAILED;
            return NULL;
        }
        bCtx.stmt      = s1;
        bCtx.conn      = thread->conn;
        bCtx.dbName    = argRenameDb(dbName);
        bCtx.stbChange = thread->stbChange;
    }

    /* Parquet STMT handles — initialized once per thread on first .par file encountered */
    TAOS_STMT  *parquetStmt  = NULL;
    TAOS_STMT2 *parquetStmt2 = NULL;

    int retryCount   = argRetryCount();
    int retrySleepMs = argRetrySleepMs();

    // In STMT2 multi-table mode, checkpoint records must not be written until
    // the data is actually flushed to the server (stmt2FlushMultiTableSlots).
    // We buffer pending file paths here and drain them after each successful flush.
    // Max capacity = STMT2_MULTI_TABLE_PENDING (one path per pending slot).
    const char *mtPendingCkpt[STMT2_MULTI_TABLE_PENDING];
    int         mtPendingCkptCnt = 0;

    /* ----- per-file restore loop ----- */
    for (int i = 0; i < thread->fileCnt; i++) {
        if (g_interrupted) {
            // Flush checkpoint buffer on interrupt
            flushCkptBuffer();
            if (thread->code == TSDB_CODE_SUCCESS) thread->code = TSDB_CODE_BCK_USER_CANCEL;
            break;
        }

        const char *filePath = thread->files[i];

        // skip if already restored (resume support; only active with -C / --checkpoint)
        if (argCheckpoint() && isRestoreDone(filePath)) {
            logDebug("restore thread %d: skip already restored: %s", thread->index, filePath);
            atomic_add_fetch_64(&g_stats.dataFilesSkipped, 1);
            atomic_add_fetch_64(&g_stats.dataFilesTotal, 1);
            // count skip as progress so the bar keeps advancing
            atomic_add_fetch_64(&g_progress.ctbDoneCur, 1);
            continue;
        }

        logDebug("restore data thread %d: file %d/%d: %s",
                thread->index, i + 1, thread->fileCnt, filePath);

        // Dispatch on file extension: .dat → binary-taos, .par → parquet
        int   code    = TSDB_CODE_SUCCESS;
        int   pathLen = strlen(filePath);
        bool  isPar   = (pathLen > 4 &&
                         strcmp(filePath + pathLen - 4, ".par") == 0);
        int64_t fileRows = 0;  // rows restored in this single file

        // Retry loop: on transient network/server errors replace the connection
        // and retry the file, consistent with the backup-side retry logic.
        int attempt = 0;
        while (1) {
            s2Ctx.lastCallFlushed = false;

            if (isPar) {
                if (stmtVer == STMT_VERSION_2) {
                    code = restoreOneParquetFileV2(&parquetStmt2, thread->conn, dbName, filePath,
                                                   thread->stbChange, &fileRows);
                } else {
                    code = restoreOneParquetFile(&parquetStmt, thread->conn, dbName, filePath,
                                                 thread->stbChange, &fileRows);
                }
            } else if (stmtVer == STMT_VERSION_2) {
                int64_t rowsBefore = s2Ctx.totalRows;
                code = restoreOneDataFileV2(&s2Ctx, filePath);
                fileRows = s2Ctx.totalRows - rowsBefore;
            } else {
                int64_t rowsBefore = bCtx.totalRows;
                code = restoreOneDataFile(&bCtx, filePath);
                fileRows = bCtx.totalRows - rowsBefore;
            }

            if (code == TSDB_CODE_SUCCESS) break;
            if (!errorCodeCanRetry(code) || attempt >= retryCount || g_interrupted) break;

            // Transient error: replace the broken connection and reset STMT state
            attempt++;
            logInfo("restore thread %d: retry file (attempt %d): %s (code=0x%08X)",
                    thread->index, attempt, filePath, code);
            releaseConnectionBad(thread->conn);
            thread->conn = getConnection(&code);
            if (!thread->conn) break;

            // Propagate new connection into both STMT contexts
            s2Ctx.conn = thread->conn;
            bCtx.conn  = thread->conn;

            // Reset STMT handles bound to the old connection
            if (!isPar) {
                if (stmtVer == STMT_VERSION_2) stmt2ResetOnError(&s2Ctx);
                else if (!stmtResetOnError(&bCtx)) {
                    logError("restore thread %d: cannot recover STMT1 after retry", thread->index);
                    break;
                }
            }
            sleepMs(retrySleepMs);
        }

        atomic_add_fetch_64(&g_stats.dataFilesTotal, 1);
        if (code != TSDB_CODE_SUCCESS) {
            logError("restore data file failed(%d): %s", code, filePath);
            atomic_add_fetch_64(&g_stats.dataFilesFailed, 1);
            if (thread->code == TSDB_CODE_SUCCESS) {
                thread->code = code;  // capture first error
            }
            // After a binary STMT failure the stmt may be in an error state.
            // Reset it so subsequent files can still be attempted.
            if (!isPar) {
                if (stmtVer == STMT_VERSION_2) {
                    stmt2ResetOnError(&s2Ctx);
                } else {
                    if (!stmtResetOnError(&bCtx)) {
                        logError("restore thread %d: cannot recover STMT1, aborting", thread->index);
                        break;
                    }
                }
            }
            // continue with next file (best effort)
        } else {
            // ---- Checkpoint handling ----
            // STMT2 multi-table mode: data may still be in pending slots (not yet
            // sent to the server).  Only record checkpoint after a confirmed flush.
            bool isMtFile = (stmtVer == STMT_VERSION_2 && s2Ctx.multiTable && !isPar);
            if (isMtFile) {
                // Buffer the path; drain after confirmed flush.
                if (mtPendingCkptCnt < STMT2_MULTI_TABLE_PENDING) {
                    mtPendingCkpt[mtPendingCkptCnt++] = filePath;
                }
                // If a threshold flush happened inside restoreOneDataFileV2, drain
                if (s2Ctx.lastCallFlushed) {
                    for (int k = 0; k < mtPendingCkptCnt; k++) {
                        insertCkptHash(mtPendingCkpt[k]);
                        markRestoreDone(mtPendingCkpt[k]);
                    }
                    mtPendingCkptCnt = 0;
                }
            } else {
                // Single-table or parquet: data is already in the server, safe to checkpoint now
                insertCkptHash(filePath);
                markRestoreDone(filePath);
            }

            // accumulate rows immediately so the progress display is real-time
            if (fileRows > 0) atomic_add_fetch_64(&g_stats.totalRows, fileRows);
            // count completed file for progress display
            atomic_add_fetch_64(&g_progress.ctbDoneCur, 1);
            // accumulate actual processed bytes for File Size summary
            {
                int64_t fsz = 0;
                if (taosStatFile(filePath, &fsz, NULL, NULL) == 0 && fsz > 0)
                    atomic_add_fetch_64(&g_stats.dataFilesSizeBytes, fsz);
            }
        }
    }

    /* ----- post-loop flushes ----- */

    /* STMT1: flush remaining cross-file accumulated rows */
    if (stmtVer == STMT_VERSION_1 && bCtx.pendingRows > 0 && bCtx.stmt) {
        int flushCode = taos_stmt_execute(bCtx.stmt);
        if (flushCode != 0) {
            logError("restore thread %d: final STMT1 flush failed: %s",
                     thread->index, taos_stmt_errstr(bCtx.stmt));
            if (thread->code == TSDB_CODE_SUCCESS) thread->code = TSDB_CODE_BCK_STMT_FAILED;
        }
        bCtx.pendingRows = 0;
    }

    /* STMT2: flush any pending multi-table slots that didn't reach the threshold */
    if (stmtVer == STMT_VERSION_2 && s2Ctx.multiTable && s2Ctx.numPending > 0) {
        int64_t rowsBeforePostFlush = s2Ctx.totalRows;
        int flushCode = stmt2FlushMultiTableSlots(&s2Ctx);
        if (flushCode != 0 && thread->code == TSDB_CODE_SUCCESS)
            thread->code = flushCode;
        /* Update global stats for rows committed in this final flush. */
        int64_t finalFlushed = s2Ctx.totalRows - rowsBeforePostFlush;
        if (finalFlushed > 0) atomic_add_fetch_64(&g_stats.totalRows, finalFlushed);
        /* Drain any remaining deferred checkpoint paths now that data is confirmed */
        if (flushCode == TSDB_CODE_SUCCESS) {
            for (int k = 0; k < mtPendingCkptCnt; k++) {
                insertCkptHash(mtPendingCkpt[k]);
                markRestoreDone(mtPendingCkpt[k]);
            }
        }
        mtPendingCkptCnt = 0;
    }

    /* ----- cleanup ----- */
    /* STMT2 */
    if (s2Ctx.colBinds) {
        stmt2FreeColBuffers(&s2Ctx);
        taosMemoryFree(s2Ctx.colBinds);
    }
    /* Free any remaining pending slots (e.g. if interrupted) */
    if (s2Ctx.pendingSlots) {
        for (int i = 0; i < s2Ctx.numPending; i++) stmt2FreeSlot(&s2Ctx.pendingSlots[i]);
        taosMemoryFree(s2Ctx.pendingSlots);
        s2Ctx.pendingSlots = NULL;
    }
    if (s2Ctx.stmt2) { taos_stmt2_close(s2Ctx.stmt2); s2Ctx.stmt2 = NULL; }

    /* STMT1 */
    if (bCtx.bindArray) {
        freeBindArray(bCtx.bindArray, bCtx.bindArrayCap);
        taosMemoryFree(bCtx.bindArray);
    }
    if (bCtx.stmt) {
        taos_stmt_close(bCtx.stmt);
        bCtx.stmt = NULL;
    }

    /* Parquet stmt handles (reused across .par files, closed once here) */
    if (parquetStmt)  { taos_stmt_close(parquetStmt);   parquetStmt  = NULL; }
    if (parquetStmt2) { taos_stmt2_close(parquetStmt2); parquetStmt2 = NULL; }

    // Flush thread-local checkpoint buffer before thread exits
    flushCkptBuffer();

    logInfo("data thread %d finished for %s.%s", thread->index, dbName, stbName4log);
    return NULL;
}


//
// -------------------------------------- FILE SCAN -----------------------------------------
//

//
// Find all data .dat files for a given STB under {outPath}/{dbName}/{stbName}_dataN/
//
static char** findDataFiles(const char *dbName, const char *stbName, int *totalCount) {
    *totalCount = 0;
    char *outPath = argOutPath();
    char dbDir[MAX_PATH_LEN];
    snprintf(dbDir, sizeof(dbDir), "%s/%s", outPath, dbName);

    // scan for directories matching {stbName}_dataN
    TdDirPtr dir = taosOpenDir(dbDir);
    if (dir == NULL) {
        logError("open db dir failed: %s", dbDir);
        return NULL;
    }

    char prefix[TSDB_TABLE_NAME_LEN + 16];
    snprintf(prefix, sizeof(prefix), "%s_data", stbName);
    int prefixLen = strlen(prefix);

    int capacity = 64;
    char **files = (char **)taosMemoryCalloc(capacity + 1, sizeof(char *));
    if (!files) {
        taosCloseDir(&dir);
        return NULL;
    }

    TdDirEntryPtr entry;
    while ((entry = taosReadDir(dir)) != NULL) {
        char *entryName = taosGetDirEntryName(entry);
        if (entryName[0] == '.') continue;

        // match data directories: {stbName}_dataN
        if (strncmp(entryName, prefix, prefixLen) != 0) continue;

        // check if it's a directory
        char subDir[MAX_PATH_LEN];
        snprintf(subDir, sizeof(subDir), "%s/%s", dbDir, entryName);
        
        if (!taosDirExist(subDir)) continue;

        // scan .dat files in this sub-directory
        TdDirPtr subDirPtr = taosOpenDir(subDir);
        if (subDirPtr == NULL) continue;

        TdDirEntryPtr subEntry;
        while ((subEntry = taosReadDir(subDirPtr)) != NULL) {
            char *subEntryName = taosGetDirEntryName(subEntry);
            if (subEntryName[0] == '.') continue;
            
            int nameLen = strlen(subEntryName);
            /* accept both .dat (binary-taos) and .par (parquet) files */
            bool isDat = (nameLen > 4 && strcmp(subEntryName + nameLen - 4, ".dat") == 0);
            bool isPar = (nameLen > 4 && strcmp(subEntryName + nameLen - 4, ".par") == 0);
            if (!isDat && !isPar) continue;

            if (*totalCount >= capacity) {
                capacity *= 2;
                char **tmp = (char **)taosMemoryRealloc(files, (capacity + 1) * sizeof(char *));
                if (!tmp) {
                    freeArrayPtr(files);
                    taosCloseDir(&subDirPtr);
                    taosCloseDir(&dir);
                    return NULL;
                }
                files = tmp;
            }

            char fullPath[MAX_PATH_LEN];
            snprintf(fullPath, sizeof(fullPath), "%s/%s", subDir, subEntryName);
            files[*totalCount] = taosStrdup(fullPath);
            (*totalCount)++;
        }

        taosCloseDir(&subDirPtr);
    }
    files[*totalCount] = NULL;

    taosCloseDir(&dir);
    return files;
}


//
// Restore data for one super table (parallel threads)
//
static int restoreStbData(DBInfo *dbInfo, const char *stbName, StbChangeMap *changeMap) {
    int code = TSDB_CODE_SUCCESS;
    const char *dbName = dbInfo->dbName;

    // Find all data files for this STB
    int fileCnt = 0;
    char **dataFiles = findDataFiles(dbName, stbName, &fileCnt);
    if (dataFiles == NULL || fileCnt == 0) {
        logInfo("no data files found for %s.%s", dbName, stbName);
        if (dataFiles) freeArrayPtr(dataFiles);
        return TSDB_CODE_SUCCESS;
    }

    logDebug("found %d data files for %s.%s", fileCnt, dbName, stbName);

    // update progress: how many files this STB/NTB has
    g_progress.ctbTotalCur = fileCnt;
    atomic_add_fetch_64(&g_progress.ctbTotalAll, fileCnt);
    atomic_store_64(&g_progress.ctbDoneCur, 0);

    // Determine thread count
    int threadCnt = argDataThread();
    if (fileCnt < threadCnt) {
        threadCnt = fileCnt;
    }

    logInfo("[%lld/%lld] db: %s  [%lld/%lld] stb: %s  data start  file: %d  threads: %d",
            (long long)g_progress.dbIndex, (long long)g_progress.dbTotal, dbName,
            (long long)g_progress.stbIndex, (long long)g_progress.stbTotal, stbName,
            fileCnt, threadCnt);

    // Allocate threads
    RestoreDataThread *threads = (RestoreDataThread *)taosMemoryCalloc(threadCnt, sizeof(RestoreDataThread));
    if (threads == NULL) {
        freeArrayPtr(dataFiles);
        return TSDB_CODE_BCK_MALLOC_FAILED;
    }

    // Distribute files across threads (round-robin)
    // First, allocate file arrays for each thread
    int *fileCounts = (int *)taosMemoryCalloc(threadCnt, sizeof(int));
    char ***threadFiles = (char ***)taosMemoryCalloc(threadCnt, sizeof(char **));
    if (!fileCounts || !threadFiles) {
        taosMemoryFree(fileCounts);
        taosMemoryFree(threadFiles);
        taosMemoryFree(threads);
        freeArrayPtr(dataFiles);
        return TSDB_CODE_BCK_MALLOC_FAILED;
    }

    // Count files per thread
    for (int i = 0; i < fileCnt; i++) {
        fileCounts[i % threadCnt]++;
    }

    // Allocate per-thread file arrays
    for (int t = 0; t < threadCnt; t++) {
        threadFiles[t] = (char **)taosMemoryCalloc(fileCounts[t] + 1, sizeof(char *));
        if (!threadFiles[t]) {
            for (int j = 0; j < t; j++) taosMemoryFree(threadFiles[j]);
            taosMemoryFree(threadFiles);
            taosMemoryFree(fileCounts);
            taosMemoryFree(threads);
            freeArrayPtr(dataFiles);
            return TSDB_CODE_BCK_MALLOC_FAILED;
        }
        fileCounts[t] = 0; // reset for filling
    }

    // Distribute files
    for (int i = 0; i < fileCnt; i++) {
        int t = i % threadCnt;
        threadFiles[t][fileCounts[t]] = dataFiles[i];
        fileCounts[t]++;
    }

    // Detect schema change for this super table (skip for normal tables "_ntb")
    StbChange *stbChange = NULL;
    if (changeMap && strcmp(stbName, NORMAL_TABLE_DIR) != 0) {
        // Get a connection to query server schema
        int scConnCode = TSDB_CODE_FAILED;
        TAOS *schemaConn = getConnection(&scConnCode);
        if (schemaConn) {
            int scCode = addStbChanged(changeMap, schemaConn, dbName, stbName);
            if (scCode != 0) {
                logWarn("schema change detection failed for %s.%s, proceeding without partial write", dbName, stbName);
            }
            stbChange = findStbChange(changeMap, stbName);
            releaseConnection(schemaConn);
        }
    }

    // Setup and create threads
    StbInfo stbInfo;
    memset(&stbInfo, 0, sizeof(StbInfo));
    stbInfo.dbInfo = dbInfo;
    stbInfo.stbName = stbName;

    for (int i = 0; i < threadCnt; i++) {
        threads[i].dbInfo    = dbInfo;
        threads[i].stbInfo   = &stbInfo;
        threads[i].index     = i + 1;
        threads[i].stbChange = stbChange;  // shared across threads (read-only)
        threads[i].conn    = getConnection(&code);
        if (!threads[i].conn) {
            for (int j = 0; j < i; j++) {
                releaseConnection(threads[j].conn);
            }
            for (int t = 0; t < threadCnt; t++) taosMemoryFree(threadFiles[t]);
            taosMemoryFree(threadFiles);
            taosMemoryFree(fileCounts);
            taosMemoryFree(threads);
            freeArrayPtr(dataFiles);
            return code;
        }
        threads[i].files   = threadFiles[i];
        threads[i].fileCnt = fileCounts[i];

        if (pthread_create(&threads[i].pid, NULL, restoreDataThread, (void *)&threads[i]) != 0) {
            logError("create restore data thread failed(%s) for stb: %s.%s",
                     strerror(errno), dbName, stbName);
            // Join already-started threads before freeing shared state
            for (int j = 0; j < i; j++) {
                pthread_join(threads[j].pid, NULL);
                releaseConnection(threads[j].conn);
            }
            // Release connections for threads that were never started
            for (int j = i; j <= i; j++) {
                releaseConnection(threads[j].conn);
            }
            // cleanup
            for (int t = 0; t < threadCnt; t++) taosMemoryFree(threadFiles[t]);
            taosMemoryFree(threadFiles);
            taosMemoryFree(fileCounts);
            taosMemoryFree(threads);
            freeArrayPtr(dataFiles);
            return TSDB_CODE_BCK_CREATE_THREAD_FAILED;
        }
    }

    // Wait threads and collect first error
    for (int i = 0; i < threadCnt; i++) {
        pthread_join(threads[i].pid, NULL);
        releaseConnection(threads[i].conn);
        if (code == TSDB_CODE_SUCCESS && threads[i].code != TSDB_CODE_SUCCESS) {
            code = threads[i].code;
        }
    }

    // Cleanup
    for (int t = 0; t < threadCnt; t++) {
        taosMemoryFree(threadFiles[t]);
    }
    taosMemoryFree(threadFiles);
    taosMemoryFree(fileCounts);
    taosMemoryFree(threads);

    // Free the dataFiles array but NOT the strings (they were passed to threads)
    // Actually the strings are still owned by dataFiles, so free normally
    freeArrayPtr(dataFiles);

    return code;
}


//
// -------------------------------------- MAIN -----------------------------------------
//

//
// Get STB names from backup directory (same as restoreMeta)
//
static char** getBackupStbNamesForData(const char *dbName, int *code) {
    *code = TSDB_CODE_SUCCESS;

    char *outPath = argOutPath();
    char dbDir[MAX_PATH_LEN];
    snprintf(dbDir, sizeof(dbDir), "%s/%s", outPath, dbName);

    TdDirPtr dir = taosOpenDir(dbDir);
    if (dir == NULL) {
        logError("open backup db dir failed: %s", dbDir);
        *code = TSDB_CODE_BCK_OPEN_DIR_FAILED;
        return NULL;
    }

    int capacity = 16;
    int count = 0;
    char **names = (char **)taosMemoryCalloc(capacity + 1, sizeof(char *));
    if (!names) {
        taosCloseDir(&dir);
        *code = TSDB_CODE_BCK_MALLOC_FAILED;
        return NULL;
    }

    // Look for {stbName}.csv files (schema files identify STBs)
    TdDirEntryPtr entry;
    while ((entry = taosReadDir(dir)) != NULL) {
        char *entryName = taosGetDirEntryName(entry);
        if (entryName[0] == '.') continue;
        
        int nameLen = strlen(entryName);
        if (nameLen > 4 && strcmp(entryName + nameLen - 4, ".csv") == 0) {
            if (count >= capacity) {
                capacity *= 2;
                char **tmp = (char **)taosMemoryRealloc(names, (capacity + 1) * sizeof(char *));
                if (!tmp) {
                    freeArrayPtr(names);
                    taosCloseDir(&dir);
                    *code = TSDB_CODE_BCK_MALLOC_FAILED;
                    return NULL;
                }
                names = tmp;
            }
            char stbName[TSDB_TABLE_NAME_LEN] = {0};
            int stbNameLen = nameLen - 4;
            if (stbNameLen >= TSDB_TABLE_NAME_LEN) stbNameLen = TSDB_TABLE_NAME_LEN - 1;
            memcpy(stbName, entryName, stbNameLen);
            stbName[stbNameLen] = '\0';
            names[count++] = taosStrdup(stbName);
        }
    }
    names[count] = NULL;

    taosCloseDir(&dir);
    return names;
}


//
// restore database data
//
int restoreDatabaseData(const char *dbName) {
    int code = TSDB_CODE_SUCCESS;

    // Reset DATA-phase progress counters so META-phase accumulation in
    // ctbDoneAll / ctbTotalAll doesn't corrupt ETA and speed calculations.
    atomic_store_64(&g_progress.ctbDoneAll,  0);
    atomic_store_64(&g_progress.ctbTotalAll, 0);
    atomic_store_64(&g_progress.ctbDoneCur,  0);
    g_progress.ctbTotalCur = 0;
    g_progress.startMs     = taosGetTimestampMs();

    // load checkpoint for resume support
    loadRestoreCheckpoint(dbName);

    // Initialize schema change map for this database
    StbChangeMap changeMap;
    stbChangeMapInit(&changeMap);

    //
    // super tables data
    //
    int stbCode = TSDB_CODE_SUCCESS;
    char **stbNames = getBackupStbNamesForData(dbName, &stbCode);
    if (stbNames == NULL) {
        if (stbCode != TSDB_CODE_SUCCESS) {
            stbChangeMapDestroy(&changeMap);
            freeRestoreCheckpoint();
            return stbCode;
        }
        // No STBs found — nothing to restore; clean up checkpoint.
        deleteRestoreCheckpoint();
        stbChangeMapDestroy(&changeMap);
        freeRestoreCheckpoint();
        return TSDB_CODE_SUCCESS;
    }

    DBInfo dbInfo;
    dbInfo.dbName = dbName;

    // count STBs for progress (+1 for NTB phase only if NTB dir exists)
    int stbRawCount = 0;
    for (int k = 0; stbNames[k] != NULL; k++) stbRawCount++;
    char ntbDir[MAX_PATH_LEN];
    snprintf(ntbDir, sizeof(ntbDir), "%s/%s/" NORMAL_TABLE_DIR "_data0", argOutPath(), dbName);
    bool hasNtb = taosDirExist(ntbDir);
    g_progress.stbTotal = stbRawCount + (hasNtb ? 1 : 0);
    g_progress.stbIndex = 0;

    for (int i = 0; stbNames[i] != NULL; i++) {
        if (g_interrupted) {
            code = TSDB_CODE_BCK_USER_CANCEL;
            break;
        }

        // update progress: which STB we're restoring
        g_progress.stbIndex++;
        snprintf(g_progress.stbName, PROGRESS_STB_NAME_LEN, "%s", stbNames[i]);
        g_progress.ctbTotalCur = 0;
        atomic_store_64(&g_progress.ctbDoneCur, 0);

        code = restoreStbData(&dbInfo, stbNames[i], &changeMap);

        // accumulate completed files
        int64_t doneCur = g_progress.ctbDoneCur;
        atomic_add_fetch_64(&g_progress.ctbDoneAll, doneCur);
        atomic_store_64(&g_progress.ctbDoneCur, 0);

        if (code != TSDB_CODE_SUCCESS) {
            logError("restore stb data failed(0x%08X): %s.%s", code, dbName, stbNames[i]);
            freeArrayPtr(stbNames);
            stbChangeMapDestroy(&changeMap);
            freeRestoreCheckpoint();
            return code;
        }
    }

    freeArrayPtr(stbNames);

    if (code != TSDB_CODE_SUCCESS) {
        stbChangeMapDestroy(&changeMap);
        freeRestoreCheckpoint();
        return code;
    }

    //
    // normal tables data (no schema change detection for normal tables)
    //
    if (!hasNtb) {
        logDebug("no normal table data dir for db: %s, skipping", dbName);
        g_progress.stbIndex++;  // keep progress counter consistent
        goto ntb_done;
    }

    // update progress for NTB phase
    g_progress.stbIndex++;
    snprintf(g_progress.stbName, PROGRESS_STB_NAME_LEN, "(ntb)");
    g_progress.ctbTotalCur = 0;
    atomic_store_64(&g_progress.ctbDoneCur, 0);

    code = restoreStbData(&dbInfo, NORMAL_TABLE_DIR, NULL);

    // accumulate NTB files
    {
        int64_t ntbDone = g_progress.ctbDoneCur;
        atomic_add_fetch_64(&g_progress.ctbDoneAll, ntbDone);
        atomic_store_64(&g_progress.ctbDoneCur, 0);
    }

    if (code != TSDB_CODE_SUCCESS) {
        logError("restore normal table data failed(%d): %s", code, dbName);
        stbChangeMapDestroy(&changeMap);
        freeRestoreCheckpoint();
        return code;
    }

ntb_done:
    // All data restored successfully.  Delete the checkpoint file so that
    // the next restore run always starts fresh instead of skipping everything.
    deleteRestoreCheckpoint();
    stbChangeMapDestroy(&changeMap);
    freeRestoreCheckpoint();
    return code;
}
