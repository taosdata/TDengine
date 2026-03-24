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
    
#include "backupData.h"
#include "backupMeta.h"
#include "storageTaos.h"
#include "storageParquet.h"
#include "bckPool.h"
#include "bckDb.h"
#include "bckProgress.h"

volatile int64_t g_backDataFiles = 0;

// When true the current backup session is resuming an interrupted previous run;
// existing .dat files are skipped (resume mode).  When false this is a fresh
// run and every table is backed up regardless of whether a .dat file already
// exists from a previous completed run.
static bool g_backResumeMode = false;

// Build the path of the per-database "backup complete" sentinel file.
// The file is created when a database backup finishes successfully and
// deleted at the start of the next run so the next run knows to start fresh.
static void backCompleteFlagPath(const char *dbName, char *buf, int bufsz) {
    snprintf(buf, bufsz, "%s/%s/backup_complete.flag", argOutPath(), dbName);
}

//
// -------------------------------------- UTIL -----------------------------------------
//


int genBackTableSql(const char *dbName, const char *tableName, char *sql, int len) {
    char *timeFilter = argTimeFilter();
    if (timeFilter == NULL) {
        // no time filter
        timeFilter = "";
    }
    
    snprintf(sql, len, "SELECT * FROM `%s`.`%s` %s", dbName, tableName, timeFilter);
    logDebug(" generate backup table sql: %s", sql);

    return TSDB_CODE_SUCCESS;
}

//
// -------------------------------------- DATA -----------------------------------------
//


//
// back child table data on thread group
//
int backChildTableData(DataThread* thread, const char *childTableName) {
    int code = TSDB_CODE_FAILED;
    const char* dbName = thread->dbInfo->dbName;
    const char* stbName = thread->stbInfo->stbName;
    StorageFormat format = argStorageFormat();

    // Use pre-built directory path (created once per stb in backDataThread)
    const char *ext = (format == BINARY_PARQUET) ? "par" : "dat";
    char pathFile[MAX_PATH_LEN];
    snprintf(pathFile, sizeof(pathFile), "%s/%s.%s", thread->stbDirPath, childTableName, ext);

    // global file count
    atomic_add_fetch_64(&g_backDataFiles, 1);

    // Skip if already backed up, but only in resume mode (interruped previous
    // run).  In a fresh run (after a successful previous run) we always
    // overwrite so the backup reflects the current database state.
    if (g_backResumeMode && taosCheckExistFile(pathFile)) {
        logDebug("skip already backed up: %s", childTableName);
        atomic_add_fetch_64(&g_stats.dataFilesSkipped, 1);
        atomic_add_fetch_64(&g_stats.dataFilesTotal, 1);
        return TSDB_CODE_SUCCESS;
    }

    // query sql
    char sql[512] = {0};
    code = genBackTableSql(dbName, childTableName, sql, sizeof(sql));
    if (code != TSDB_CODE_SUCCESS) {
        logError("generate backup table sql failed(%d): %s.%s", code, dbName, childTableName);
        return code;
    }

    //
    // write to .tmp first, then rename on success
    //
    char tmpFile[MAX_PATH_LEN];
    snprintf(tmpFile, sizeof(tmpFile), "%s.tmp", pathFile);
    int64_t rows = 0;
    code = queryWriteBinaryEx(thread->conn, sql, format, tmpFile, thread->writeBuf, thread->writeBufCap, &rows);
    if (code == TSDB_CODE_SUCCESS) {
        if (rows == 0) {
            // No data (empty table or no rows in time range): discard the tmp
            // file and skip creating a .dat file entirely.  This avoids 1000s of
            // zero-row files when backing up sparse / high-cardinality datasets.
            taosRemoveFile(tmpFile);
            logDebug("skip empty table (0 rows): %s.%s", dbName, childTableName);
            return TSDB_CODE_SUCCESS;
        }
        if (taosRenameFile(tmpFile, pathFile) != 0) {
            logError("rename tmp file failed: %s -> %s, errno: %d", tmpFile, pathFile, errno);
            code = TSDB_CODE_FAILED;
        } else {
            atomic_add_fetch_64(&g_stats.totalRows, rows);
            atomic_add_fetch_64(&g_stats.childTablesTotal, 1);
        }
    } else {
        // remove incomplete tmp file
        taosRemoveFile(tmpFile);
    }

    atomic_add_fetch_64(&g_stats.dataFilesTotal, 1);
    if (code != TSDB_CODE_SUCCESS) {
        atomic_add_fetch_64(&g_stats.dataFilesFailed, 1);
    }

    return code;
}

//
// back data thread
//
static void* backDataThread(void *arg) {
    int retryCount   = argRetryCount();
    int retrySleepMs = argRetrySleepMs();
    int code = TSDB_CODE_SUCCESS;

    DataThread * thread = (DataThread *)arg;
    logInfo("data thread %d started for %s.%s (offset=%d, limit=%d)",
            thread->index, thread->dbInfo->dbName, thread->stbInfo->stbName, thread->offset, thread->limit);

    // Allocate thread-level reusable 4MB write buffer (space-for-time optimization)
    thread->writeBuf = (char *)taosMemoryMalloc(4 * 1024 * 1024);
    thread->writeBufCap = thread->writeBuf ? (4 * 1024 * 1024) : 0;

    // Pre-build directory path for this stb and create it once (optimization 3+5)
    StorageFormat format = argStorageFormat();
    code = obtainFileName(BACK_DIR_DATA, thread->dbInfo->dbName, thread->stbInfo->stbName,
                          "", thread->index, 0, format, thread->stbDirPath, sizeof(thread->stbDirPath));
    if (code == TSDB_CODE_SUCCESS && !taosDirExist(thread->stbDirPath)) {
        taosMkDir(thread->stbDirPath);
    }
    thread->stbDirCreated = true;

    // Aggregate function:
    //   last(ts)     – no time filter: benefits from last-value cache when enabled, O(n_tables)
    //   last_row(ts) – with time filter: only inspects the last row in range, faster than full scan
    const char *tf = argTimeFilter();
    bool hasTf = (tf && tf[0]);
    char innerWhere[10240] = "";
    {
        char tfPart[256] = "", inPart[7800] = "";
        if (hasTf)
            snprintf(tfPart, sizeof(tfPart), "%s", tf + 6);  // strip leading "WHERE "
        if (argSpecTables() && !argStbNameInSpecTables(thread->stbInfo->stbName))
            argBuildInClause("tbname", inPart, sizeof(inPart));
        bool hasIn = (inPart[0] != '\0');
        if (hasTf && hasIn)
            snprintf(innerWhere, sizeof(innerWhere), "WHERE %s AND %s", tfPart, inPart);
        else if (hasTf)
            snprintf(innerWhere, sizeof(innerWhere), "WHERE %s", tfPart);
        else if (hasIn)
            snprintf(innerWhere, sizeof(innerWhere), "WHERE %s", inPart);
    }
    char sql[12288] = {0};
    snprintf(sql, sizeof(sql),
             "SELECT tbname FROM ("
             "SELECT %s lts, tbname FROM `%s`.`%s` %s GROUP BY tbname"
             ") WHERE lts IS NOT NULL ORDER BY tbname LIMIT %d OFFSET %d;",
             hasTf ? "last_row(ts)" : "last(ts)",
             thread->dbInfo->dbName,
             thread->stbInfo->stbName,
             innerWhere,
             thread->limit,
             thread->offset);
    
    // query child table names — with retry on transient network errors
    TAOS* conn = getConnection(&thread->code);
    if (!conn) {
        return NULL;
    }
    TAOS_RES *res = NULL;
    {
        int retryCount   = argRetryCount();
        int retrySleepMs = argRetrySleepMs();
        int attempt = 0;
        while (1) {
            res = taos_query(conn, sql);
            thread->code = taos_errno(res);
            if (thread->code == TSDB_CODE_SUCCESS) break;
            if (res) { taos_free_result(res); res = NULL; }
            if (!errorCodeCanRetry(thread->code) || attempt >= retryCount || g_interrupted) break;
            attempt++;
            logInfo("retry query child table names: %s.%s, attempt: %d", thread->dbInfo->dbName, thread->stbInfo->stbName, attempt);
            releaseConnectionBad(conn);
            conn = getConnection(&thread->code);
            if (!conn) return NULL;
            sleepMs(retrySleepMs);
        }
    }
    if (res == NULL || thread->code) {
        logError("query child table names failed(0x%08X %s): %s", thread->code, taos_errstr(res), sql);
        if (res) taos_free_result(res);
        releaseConnection(conn);
        return NULL;
    }

    // loop child tables
    TAOS_ROW row;
    int offset = thread->offset;
    char childTableName[TSDB_TABLE_NAME_LEN]= {0};
    while ((row = taos_fetch_row(res))) {
        int32_t *lens = taos_fetch_lengths(res);
        if (lens[0] >= TSDB_TABLE_NAME_LEN) {
            logWarn("child table name too long, skip. offset: %d sql=%s", offset, sql);
            offset += 1;
            continue;
        }

        memcpy(childTableName, (char *)row[0], lens[0]);
        childTableName[lens[0]] = '\0';

        logDebug("backing up child table: %s.%s.%s", thread->dbInfo->dbName, thread->stbInfo->stbName, childTableName);
        
        // support retry
        int n = 0;
        while (n < retryCount) {
            // back child table data
            code = backChildTableData(thread, childTableName);
            // check user cancelled
            if (g_interrupted) {
                code = TSDB_CODE_BCK_USER_CANCEL;
                break;
            }

            // check code
            if (code == TSDB_CODE_SUCCESS) {
                // success
                break;
            }
            else if (errorCodeCanRetry(code)) {
                // can retry — evict potentially-stale connection and get a fresh one
                releaseConnectionBad(thread->conn);
                thread->conn = getConnection(&code);
                if (!thread->conn) {
                    break;
                }
                n += 1;
                logInfo("retry backup child table data: %s, times: %d", childTableName, n);
                sleepMs(retrySleepMs);
            } else {
                // not retry
                logError("backup child table data failed(%d): %s.%s", code, thread->dbInfo->dbName, childTableName);
                break;
            }
        }

        // if failed break
        if(code != TSDB_CODE_SUCCESS) {
            thread->code = code;
            break;
        }

        // count completed CTB for progress display
        atomic_add_fetch_64(&g_progress.ctbDoneCur, 1);

        // save checkpoint
        offset += 1;
    }

    taos_free_result(res);
    releaseConnection(conn);

    // Free thread-level write buffer
    if (thread->writeBuf) {
        taosMemoryFree(thread->writeBuf);
        thread->writeBuf = NULL;
    }

    thread->code = code;
    logInfo("data thread %d finished for %s.%s", thread->index, thread->dbInfo->dbName, thread->stbInfo->stbName);
    return NULL;
}

//
// split child tables to thread groups
//
DataThread * splitTaskData(StbInfo *stbInfo, int *code, int *outCount, int *totCtbs) {
    int threadCnt = *outCount;
    DBInfo *dbInfo = stbInfo->dbInfo;
    const char* dbName = stbInfo->dbInfo->dbName;
    const char* stbName = stbInfo->stbName;

    // Pre-filter: count only CTBs that have data in the backup range (方案四).
    // Build innerWhere combining -S/-E time filter and/or spec-tables IN filter;
    // -S/-E applies to the spec-tables path too.
    // Aggregate function:
    //   last(ts)     – no time filter: benefits from last-value cache when enabled, O(n_tables)
    //   last_row(ts) – with time filter: only inspects the last row in range, faster than full scan
    // HAVING with aggregate is not supported for this syntax; use subquery instead.
    const char *tf = argTimeFilter();
    bool hasTf = (tf && tf[0]);
    char innerWhere[8192] = "";
    {
        char tfPart[256] = "", inPart[3800] = "";
        if (hasTf)
            snprintf(tfPart, sizeof(tfPart), "%s", tf + 6);  // strip leading "WHERE "
        if (argSpecTables() && !argStbNameInSpecTables(stbName))
            argBuildInClause("tbname", inPart, sizeof(inPart));
        bool hasIn = (inPart[0] != '\0');
        if (hasTf && hasIn)
            snprintf(innerWhere, sizeof(innerWhere), "WHERE %s AND %s", tfPart, inPart);
        else if (hasTf)
            snprintf(innerWhere, sizeof(innerWhere), "WHERE %s", tfPart);
        else if (hasIn)
            snprintf(innerWhere, sizeof(innerWhere), "WHERE %s", inPart);
    }
    char sql[8192] = {0};
    snprintf(sql, sizeof(sql),
             "SELECT count(*) FROM ("
             "SELECT %s lts FROM `%s`.`%s` %s GROUP BY tbname"
             ") WHERE lts IS NOT NULL;",
             hasTf ? "last_row(ts)" : "last(ts)",
             dbName, stbName, innerWhere);
    int32_t tableCnt = 0;
    *code = queryValueInt(sql, 0, &tableCnt);
    if (*code != TSDB_CODE_SUCCESS) {
        return NULL;
    }
    if (totCtbs) *totCtbs = tableCnt;
    
    if (tableCnt == 0) {
        logDebug("%s.%s child table count is zero.", dbName, stbName);
        *code = TSDB_CODE_SUCCESS;
        *outCount = 0;
        return NULL;
    }

    if (tableCnt < threadCnt) {
        threadCnt = tableCnt;
    }

    // data thread
    DataThread * threads = (DataThread *)taosMemoryCalloc(threadCnt, sizeof(DataThread));
    if (threads == NULL) {
        *code = TSDB_CODE_FAILED;
        return NULL;
    }

    int remain = tableCnt % threadCnt;
    int base = tableCnt / threadCnt;
    int offset = 0;

    for(int i = 0; i < threadCnt; i++) {
        threads[i].dbInfo  = dbInfo;
        threads[i].stbInfo = stbInfo;
        threads[i].index   = i + 1;
        threads[i].limit   = base;
        if (remain > 0) {
            remain--;
            threads[i].limit += 1;
        }
        threads[i].offset  = offset;
        threads[i].conn    = getConnection(&threads[i].code);
        if (!threads[i].conn) {
            // release already-allocated connections
            for (int j = 0; j < i; j++) {
                releaseConnection(threads[j].conn);
            }
            int errCode = threads[i].code;   // save before free
            taosMemoryFree(threads);
            *code = errCode;
            return NULL;
        }
        offset += threads[i].limit;
    }

    // succ
    *outCount = threadCnt;
    *code = TSDB_CODE_SUCCESS;
    return threads;
}

//
// data create threads
//
int backStbData(StbInfo *stbInfo) {
    int code = TSDB_CODE_FAILED;
    int count = argDataThread();

    // reset global file count
    g_backDataFiles = 0;

    // split child tables to thread groups; also get total CTB count for progress
    int totCtbs = 0;
    DataThread * threads = splitTaskData(stbInfo, &code, &count, &totCtbs);
    if (threads == NULL) {
        return code;
    }

    // update progress: how many CTBs this STB has
    g_progress.ctbTotalCur  = totCtbs;
    atomic_add_fetch_64(&g_progress.ctbTotalAll, totCtbs);
    atomic_store_64(&g_progress.ctbDoneCur, 0);

    // calculate total child tables
    int totalChildTables = 0;
    for (int i = 0; i < count; i++) {
        totalChildTables += threads[i].limit;
    }
    logInfo("[%lld/%lld] db: %s  [%lld/%lld] stb: %s  data start  ctb: %d  threads: %d",
            (long long)g_progress.dbIndex, (long long)g_progress.dbTotal, stbInfo->dbInfo->dbName,
            (long long)g_progress.stbIndex, (long long)g_progress.stbTotal, stbInfo->stbName,
            totalChildTables, count);

    // create threads
    for (int i = 0; i < count; i++) {
        if(pthread_create(&threads[i].pid, NULL, backDataThread, (void *)&threads[i]) != 0) {
            logError("create backup thread failed(%s) for stb: %s.%s", strerror(errno), stbInfo->dbInfo->dbName, stbInfo->stbName);
            // Join already-started threads before freeing shared state
            for (int j = 0; j < i; j++) {
                pthread_join(threads[j].pid, NULL);
                releaseConnection(threads[j].conn);
            }
            // Release connections for threads that were never started
            for (int j = i; j < count; j++) {
                releaseConnection(threads[j].conn);
            }
            taosMemoryFree(threads);
            return TSDB_CODE_BCK_CREATE_THREAD_FAILED;
        }
    }

    // wait threads
    for (int i = 0; i < count; i++) {
        pthread_join(threads[i].pid, NULL);
        releaseConnection(threads[i].conn);
        if (code == TSDB_CODE_SUCCESS && threads[i].code != TSDB_CODE_SUCCESS) {
            code = threads[i].code;
        }
    }

    // free
    taosMemoryFree(threads);
    return code;
}


//
// --------------------------------- NORMAL TABLE DATA ----------------------------------
//

// backup one normal table data
static int backNormalOneTable(DataThread* thread, const char *tableName) {
    int code = TSDB_CODE_FAILED;
    const char* dbName = thread->dbInfo->dbName;
    StorageFormat format = argStorageFormat();
    const char *ext = (format == BINARY_TAOS) ? "dat" : "par";

    // ensure dir exists
    char dirPath[MAX_PATH_LEN];
    int dirIndex = (int)(g_backDataFiles / FOLDER_MAXFILE);
    obtainFileName(BACK_DIR_NTBDATA, dbName, NULL, NULL, 0, g_backDataFiles, format, dirPath, sizeof(dirPath));
    if (!taosDirExist(dirPath)) {
        taosMkDir(dirPath);
    }

    // file path: {outPath}/{db}/_ntb_data{N}/{tableName}.dat
    char pathFile[MAX_PATH_LEN];
    snprintf(pathFile, sizeof(pathFile), "%s/%s.%s", dirPath, tableName, ext);

    atomic_add_fetch_64(&g_backDataFiles, 1);

    // Skip only when resuming an interrupted run (see g_backResumeMode).
    if (g_backResumeMode && taosCheckExistFile(pathFile)) {
        logDebug("skip already backed up normal table: %s.%s", dbName, tableName);
        atomic_add_fetch_64(&g_stats.dataFilesSkipped, 1);
        atomic_add_fetch_64(&g_stats.dataFilesTotal, 1);
        return TSDB_CODE_SUCCESS;
    }

    // query sql
    char sql[512] = {0};
    code = genBackTableSql(dbName, tableName, sql, sizeof(sql));
    if (code != TSDB_CODE_SUCCESS) {
        logError("generate backup table sql failed(%d): %s.%s", code, dbName, tableName);
        return code;
    }

    // write to .tmp first, then rename on success
    char tmpFile[MAX_PATH_LEN];
    snprintf(tmpFile, sizeof(tmpFile), "%s.tmp", pathFile);
    int64_t rows = 0;
    code = queryWriteBinary(thread->conn, sql, format, tmpFile, &rows);
    if (code == TSDB_CODE_SUCCESS) {
        if (rows == 0) {
            // No data in time range: discard tmp file, nothing to back up.
            taosRemoveFile(tmpFile);
            logDebug("skip empty normal table (0 rows): %s.%s", dbName, tableName);
            return TSDB_CODE_SUCCESS;
        }
        if (taosRenameFile(tmpFile, pathFile) != 0) {
            logError("rename tmp file failed: %s -> %s, errno: %d", tmpFile, pathFile, errno);
            code = TSDB_CODE_FAILED;
        } else {
            atomic_add_fetch_64(&g_stats.totalRows, rows);
        }
    } else {
        taosRemoveFile(tmpFile);
    }

    atomic_add_fetch_64(&g_stats.dataFilesTotal, 1);
    if (code != TSDB_CODE_SUCCESS) {
        atomic_add_fetch_64(&g_stats.dataFilesFailed, 1);
    }

    return code;
}

// normal table data backup thread
static void* backNtbDataThread(void *arg) {
    int retryCount   = argRetryCount();
    int retrySleepMs = argRetrySleepMs();
    
    DataThread *thread = (DataThread *)arg;
    const char *dbName = thread->dbInfo->dbName;
    logInfo("normal table data thread %d started for %s (offset=%d, limit=%d)", 
            thread->index, dbName, thread->offset, thread->limit);

    // query normal table names for this thread's range
    char sql[1024] = {0};
    char specFilter[512] = "";
    if (argSpecTables()) {
        char inClause[400] = "";
        argBuildInClause("table_name", inClause, sizeof(inClause));
        snprintf(specFilter, sizeof(specFilter), " AND %s", inClause);
    }
    snprintf(sql, sizeof(sql), 
             "SELECT table_name FROM information_schema.ins_tables "
             "WHERE db_name='%s' AND stable_name IS NULL"
             " AND type NOT LIKE 'VIRTUAL%%'%s "
             "ORDER BY table_name LIMIT %d OFFSET %d;",
             dbName, specFilter, thread->limit, thread->offset);

    TAOS *conn = getConnection(&thread->code);
    if (!conn) {
        return NULL;
    }
    TAOS_RES *res = NULL;
    {
        int retryCount   = argRetryCount();
        int retrySleepMs = argRetrySleepMs();
        int attempt = 0;
        while (1) {
            res = taos_query(conn, sql);
            int32_t qcode = taos_errno(res);
            if (qcode == TSDB_CODE_SUCCESS) break;
            if (res) { taos_free_result(res); res = NULL; }
            if (!errorCodeCanRetry(qcode) || attempt >= retryCount || g_interrupted) {
                thread->code = qcode;
                break;
            }
            attempt++;
            logInfo("retry query normal table names: %s, attempt: %d", dbName, attempt);
            releaseConnectionBad(conn);
            conn = getConnection(&thread->code);
            if (!conn) return NULL;
            sleepMs(retrySleepMs);
        }
    }
    int32_t code = taos_errno(res);
    if (res == NULL || code) {
        logError("query normal table names failed(0x%08X %s): %s", code, taos_errstr(res), sql);
        if (res) taos_free_result(res);
        releaseConnection(conn);
        return NULL;
    }

    TAOS_ROW row;
    char tableName[TSDB_TABLE_NAME_LEN] = {0};
    while ((row = taos_fetch_row(res))) {
        if (g_interrupted) {
            thread->code = TSDB_CODE_BCK_USER_CANCEL;
            break;
        }

        int32_t *lens = taos_fetch_lengths(res);
        if (lens[0] >= TSDB_TABLE_NAME_LEN) continue;

        memcpy(tableName, (char *)row[0], lens[0]);
        tableName[lens[0]] = '\0';

        logDebug("backing up normal table: %s.%s", dbName, tableName);

        int n = 0;
        while (n < retryCount) {
            thread->code = backNormalOneTable(thread, tableName);
            if (g_interrupted) {
                thread->code = TSDB_CODE_BCK_USER_CANCEL;
                break;
            }

            if (thread->code == TSDB_CODE_SUCCESS) {
                // count completed NTB for progress display
                atomic_add_fetch_64(&g_progress.ctbDoneCur, 1);
                break;
            } else if (errorCodeCanRetry(thread->code)) {
                // evict potentially-stale connection and get a fresh one
                releaseConnectionBad(thread->conn);
                thread->conn = getConnection(&thread->code);
                if (!thread->conn) {
                    break;
                }
                n++;
                logInfo("retry backup normal table data: %s, times: %d", tableName, n);
                sleepMs(retrySleepMs);
            } else {
                logError("backup normal table data failed(%d): %s.%s", thread->code, dbName, tableName);
                break;
            }
        }

        if (g_interrupted) break;
    }

    taos_free_result(res);
    releaseConnection(conn);

    logInfo("normal table data thread %d finished for %s", thread->index, dbName);
    return NULL;
}

// backup all normal table data
static int backNormalTableData(DBInfo *dbInfo) {
    int code = TSDB_CODE_SUCCESS;
    const char *dbName = dbInfo->dbName;

    // count normal tables
    int32_t tableCnt = 0;
    code = getDBNormalTableCount(dbName, &tableCnt);
    if (code != TSDB_CODE_SUCCESS) return code;
    if (tableCnt == 0) {
        logDebug("no normal tables in db: %s", dbName);
        return TSDB_CODE_SUCCESS;
    }

    logInfo("backup %d normal table(s) data for db: %s", tableCnt, dbName);

    // update progress for the NTB phase (treat as one more STB-like entry)
    g_progress.stbIndex++;
    snprintf(g_progress.stbName, PROGRESS_STB_NAME_LEN, "(ntb)");
    g_progress.ctbTotalCur = tableCnt;
    atomic_add_fetch_64(&g_progress.ctbTotalAll, tableCnt);
    atomic_store_64(&g_progress.ctbDoneCur, 0);

    int threadCnt = argDataThread();
    if (tableCnt < threadCnt) threadCnt = tableCnt;

    DataThread *threads = (DataThread *)taosMemoryCalloc(threadCnt, sizeof(DataThread));
    if (!threads) return TSDB_CODE_BCK_MALLOC_FAILED;

    int remain = tableCnt % threadCnt;
    int base   = tableCnt / threadCnt;
    int offset = 0;

    // setup virtual stbInfo for normal tables
    StbInfo ntbStbInfo;
    memset(&ntbStbInfo, 0, sizeof(StbInfo));
    ntbStbInfo.dbInfo  = dbInfo;
    ntbStbInfo.stbName = NORMAL_TABLE_DIR;

    for (int i = 0; i < threadCnt; i++) {
        threads[i].dbInfo  = dbInfo;
        threads[i].stbInfo = &ntbStbInfo;
        threads[i].index   = i + 1;
        threads[i].limit   = base + (remain > 0 ? 1 : 0);
        if (remain > 0) remain--;
        threads[i].offset  = offset;
        threads[i].conn    = getConnection(&threads[i].code);
        if (!threads[i].conn) {
            for (int j = 0; j < i; j++) {
                releaseConnection(threads[j].conn);
            }
            int errCode = threads[i].code;
            taosMemoryFree(threads);
            return errCode;
        }
        offset += threads[i].limit;
    }

    for (int i = 0; i < threadCnt; i++) {
        if (pthread_create(&threads[i].pid, NULL, backNtbDataThread, (void *)&threads[i]) != 0) {
            logError("create ntb backup thread failed: %s", strerror(errno));
            // Join already-started threads before freeing shared state
            for (int j = 0; j < i; j++) {
                pthread_join(threads[j].pid, NULL);
                releaseConnection(threads[j].conn);
            }
            // Release connections for threads that were never started
            for (int j = i; j < threadCnt; j++) {
                releaseConnection(threads[j].conn);
            }
            taosMemoryFree(threads);
            return TSDB_CODE_BCK_CREATE_THREAD_FAILED;
        }
    }

    for (int i = 0; i < threadCnt; i++) {
        pthread_join(threads[i].pid, NULL);
        releaseConnection(threads[i].conn);
        if (code == TSDB_CODE_SUCCESS && threads[i].code != TSDB_CODE_SUCCESS) {
            code = threads[i].code;
        }
    }

    taosMemoryFree(threads);
    return code;
}

//
// backup database data
//
int backDatabaseData(DBInfo *dbInfo) {
    int code = TSDB_CODE_FAILED;
    const char *dbName = dbInfo->dbName;

    // Determine whether this is a fresh run or a resume of an interrupted run.
    //   - backup_complete.flag exists  → previous run finished successfully;
    //     delete the flag and run fresh (overwrite existing .dat files).
    //   - flag absent                  → previous run was interrupted;
    //     keep existing .dat files and resume from where we left off.
    char completeFlagPath[MAX_PATH_LEN];
    backCompleteFlagPath(dbName, completeFlagPath, sizeof(completeFlagPath));
    if (taosCheckExistFile(completeFlagPath)) {
        taosRemoveFile(completeFlagPath);
        g_backResumeMode = false;
        logInfo("backup db %s: previous run completed, starting fresh", dbName);
    } else {
        // Always record checkpoint data (existing .dat files act as checkpoints),
        // but only skip already-done files when the user explicitly requests resume
        // via -C / --checkpoint.
        g_backResumeMode = argCheckpoint() ? true : false;
        if (g_backResumeMode)
            logInfo("backup db %s: checkpoint mode, resuming from previous run", dbName);
        else
            logDebug("backup db %s: no complete flag, starting fresh (use -C to resume)", dbName);
    }

    // Reset DATA-phase progress counters so META-phase accumulation in
    // ctbDoneAll / ctbTotalAll doesn't corrupt ETA and speed calculations.
    atomic_store_64(&g_progress.ctbDoneAll,  0);
    atomic_store_64(&g_progress.ctbTotalAll, 0);
    atomic_store_64(&g_progress.ctbDoneCur,  0);
    g_progress.ctbTotalCur = 0;
    g_progress.startMs     = taosGetTimestampMs();

    //
    // super tables
    // 
    char ** stbNames = getDBSuperTableNames(dbName, &code);
    if (stbNames == NULL && code != TSDB_CODE_SUCCESS) {
        return code;
    }

    // count STBs for progress display (rough count before virtual/spec filtering)
    int stbRawCount = 0;
    for (int k = 0; stbNames != NULL && stbNames[k] != NULL; k++) stbRawCount++;
    g_progress.stbTotal = stbRawCount;
    g_progress.stbIndex = 0;

    int stbEffectiveIdx = 0;  // index of STBs actually processed (skips filtered ones)
    for (int i = 0; stbNames != NULL && stbNames[i] != NULL; i++) {
        // If specific tables are requested, only include stbs that either
        // (a) are directly named in specTables, or
        // (b) have child tables matching specTables.
        if (argSpecTables()) {
            bool include = false;
            char **specTbs = argSpecTables();
            // (a) direct stb name match
            for (int j = 0; !include && specTbs[j] != NULL; j++) {
                if (strcmp(stbNames[i], specTbs[j]) == 0) include = true;
            }
            // (b) matching child table
            if (!include) {
                char inClause[3800] = "";
                argBuildInClause("table_name", inClause, sizeof(inClause));
                char countSql[TSDB_MAX_SQL_LEN];
                snprintf(countSql, sizeof(countSql),
                         "SELECT count(*) FROM information_schema.ins_tables "
                         "WHERE db_name='%s' AND stable_name='%s' AND %s;",
                         dbName, stbNames[i], inClause);
                int32_t matchCount = 0;
                queryValueInt(countSql, 0, &matchCount);
                include = (matchCount > 0);
            }
            if (!include) continue;  /* skip this super table */
        }
        // Virtual super tables are views over physical tables; their data
        // lives in the referenced physical tables, not in the VSTB itself.
        // Skip data backup for virtual STBs entirely.
        if (isVirtualSuperTable(dbName, stbNames[i])) {
            logInfo("skip data backup for virtual STB: %s.%s", dbName, stbNames[i]);
            continue;
        }

        stbEffectiveIdx++;
        // update progress: which STB we're starting
        g_progress.stbIndex = stbEffectiveIdx;
        snprintf(g_progress.stbName, PROGRESS_STB_NAME_LEN, "%s", stbNames[i]);
        g_progress.ctbTotalCur = 0;
        atomic_store_64(&g_progress.ctbDoneCur, 0);

        StbInfo stbInfo;
        memset(&stbInfo, 0, sizeof(StbInfo));
        stbInfo.dbInfo = dbInfo;
        stbInfo.stbName = stbNames[i];

        code = backStbData(&stbInfo);

        // accumulate completed CTBs for global ETA
        int64_t doneCur = g_progress.ctbDoneCur;
        atomic_add_fetch_64(&g_progress.ctbDoneAll, doneCur);
        atomic_store_64(&g_progress.ctbDoneCur, 0);

        if (code != TSDB_CODE_SUCCESS) {
            freeArrayPtr(stbNames);
            return code;
        }
        logInfo("stb done: %s  ctb=%" PRId64 "  total_rows=%" PRId64,
                stbNames[i], doneCur, g_stats.totalRows);
    }

    freeArrayPtr(stbNames);

    //
    // normal tables
    //
    code = backNormalTableData(dbInfo);
    // accumulate NTB done count
    {
        int64_t ntbDone = g_progress.ctbDoneCur;
        atomic_add_fetch_64(&g_progress.ctbDoneAll, ntbDone);
        atomic_store_64(&g_progress.ctbDoneCur, 0);
        if (ntbDone > 0) {
            logInfo("ntb done: total_rows=%" PRId64, g_stats.totalRows);
        }
    }
    if (code != TSDB_CODE_SUCCESS) {
        return code;
    }

    // Backup succeeded — write the complete flag so the next run knows to start
    // fresh (overwrite) rather than skip existing files.
    if (code == TSDB_CODE_SUCCESS) {
        TdFilePtr fp = taosOpenFile(completeFlagPath, TD_FILE_WRITE | TD_FILE_CREATE | TD_FILE_TRUNC);
        if (fp) {
            taosCloseFile(&fp);
            logDebug("backup db %s: complete flag written (%s)", dbName, completeFlagPath);
        } else {
            logWarn("backup db %s: failed to write complete flag", dbName);
        }
    }

    return code;
}