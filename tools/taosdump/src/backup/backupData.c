/*
 * Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

    
#include "backupData.h"
#include "backupMeta.h"
#include "storageTaos.h"
#include "storageParquet.h"
#include "bckPool.h"
#include "bckDb.h"
#include "bckUtil.h"
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
    // Percent-encode the db name (':' '\\' etc. are illegal in Windows file names).
    char encDb[MAX_PATH_LEN] = {0};
    const char *encDbName = bckNameEncode(dbName, encDb, sizeof(encDb));
    if (encDbName == NULL) encDbName = dbName;
    snprintf(buf, bufsz, "%s/%s/backup_complete.flag", argOutPath(), encDbName);
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
    // Percent-encode the child table name (':' '\\' etc. are illegal in Windows file names).
    char encTb[TSDB_TABLE_NAME_LEN * 3 + 1] = {0};
    const char *encTbName = bckNameEncode(childTableName, encTb, sizeof(encTb));
    if (encTbName == NULL) encTbName = childTableName;
    char pathFile[MAX_PATH_LEN];
    snprintf(pathFile, sizeof(pathFile), "%s/%s.%s", thread->stbDirPath, encTbName, ext);

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
        logError("generate backup table sql failed(0x%08X, %s): %s.%s", code, bckErrMsg(code), dbName, childTableName);
        return code;
    }

    //
    // write to .tmp first, then rename on success
    //
    char tmpFile[MAX_PATH_LEN];
    snprintf(tmpFile, sizeof(tmpFile), "%s.tmp", pathFile);
    int64_t rows = 0;
    code = queryWriteBinaryEx(thread->conn, sql, format, tmpFile, thread->writeBuf, thread->writeBufCap, &rows, NULL);
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
// object name enumeration
//

// Callback invoked by backEnumerateObjects() for every enumerated name.
// Returns TSDB_CODE_SUCCESS to continue with the next name, or an error code to
// abort the run.
typedef int (*ObjectNameHandler)(DataThread *thread, const char *name);

// Enumerate object names (child tables of one super table, or normal tables of
// one database) from `sqlHead LIMIT n OFFSET m` and dispatch them to `handler`.
//
// The names are streamed from the server, so a connection that drops *after*
// taos_query() succeeded makes taos_fetch_row() return NULL.  Treating that as
// end-of-result silently skips the remaining names, and the run still reported
// SUCCESS with a truncated backup.  Here a page that breaks mid-stream is
// re-queried with OFFSET advanced past the names already handled, and an
// unrecoverable failure is returned to the caller so the run is reported FAILED.
//
// `-k` counts retries after the first attempt, like every other retry loop in
// taosdump, so `-k 0` still makes one attempt.
//
// *doneOut receives the number of names handled (backed up or skipped).
static int backEnumerateObjects(DataThread *thread, TAOS **conn, const char *sqlHead,
                                const char *what, ObjectNameHandler handler, int64_t *doneOut) {
    int retryCount   = argRetryCount();
    int retrySleepMs = argRetrySleepMs();
    int64_t done     = 0;
    int streamRetry  = 0;
    int code         = TSDB_CODE_SUCCESS;

    int sqlLen = (int)strlen(sqlHead) + 64;
    char *sql = (char *)taosMemoryMalloc(sqlLen);
    if (sql == NULL) {
        return TSDB_CODE_BCK_MALLOC_FAILED;
    }

    while (done < thread->limit) {
        // query only the not-yet-handled part of this thread's range, so a retry
        // continues after the names that were already backed up
        snprintf(sql, sqlLen, "%s LIMIT %lld OFFSET %lld;", sqlHead,
                 (long long)(thread->limit - done), (long long)(thread->offset + done));

        // page query, retried on transient errors
        TAOS_RES *res = NULL;
        int attempt = 0;
        while (1) {
            res = taos_query(*conn, sql);
            code = taos_errno(res);
            if (code == TSDB_CODE_SUCCESS) {
                break;
            }

            // copy the message before releasing the result: taos_errstr(NULL)
            // only reports the thread-local error, which may already have moved on
            char queryErr[512] = {0};
            const char *queryErrStr = res ? taos_errstr(res) : taos_errstr(NULL);
            snprintf(queryErr, sizeof(queryErr), "%s", queryErrStr ? queryErrStr : "");
            if (res) {
                taos_free_result(res);
                res = NULL;
            }
            if (!errorCodeCanRetry(code) || attempt >= retryCount || g_interrupted || g_fatalError) {
                logError("query %s names failed(0x%08X %s): %s", what, code, queryErr, sql);
                goto cleanup;
            }
            attempt++;
            logInfo("retry query %s names: %s.%s, attempt: %d", what, thread->dbInfo->dbName,
                    thread->stbInfo->stbName, attempt);
            if (bckConnLevelError(code)) {
                resetConnectionPool();
                int connCode = TSDB_CODE_SUCCESS;
                *conn = getConnection(&connCode);
                if (*conn == NULL) {
                    // keep the query failure as the reported reason
                    if (connCode != TSDB_CODE_SUCCESS) {
                        code = connCode;
                    }
                    goto cleanup;
                }
            }
            sleepMs(retrySleepMs);
        }

        // dispatch the names of this page
        int64_t page = 0;
        TAOS_ROW row;
        while ((row = taos_fetch_row(res)) != NULL) {
            int32_t *lens = taos_fetch_lengths(res);
            if (lens == NULL || lens[0] < 0 || lens[0] >= TSDB_TABLE_NAME_LEN) {
                // counted as handled so the retry below never re-reads this page
                logWarn("%s name too long, skip. offset: %lld, sql: %s", what,
                        (long long)(thread->offset + done), sql);
            } else {
                char name[TSDB_TABLE_NAME_LEN] = {0};
                memcpy(name, (char *)row[0], lens[0]);
                name[lens[0]] = '\0';

                int hcode = handler(thread, name);
                if (hcode != TSDB_CODE_SUCCESS) {
                    taos_free_result(res);
                    code = hcode;
                    goto cleanup;
                }
            }
            done++;
            page++;
        }

        // taos_fetch_row() also returns NULL on error: only a clean end of the
        // page means every name was enumerated
        int fetchCode = taos_errno(res);
        char fetchErr[512] = {0};
        if (fetchCode != TSDB_CODE_SUCCESS) {
            const char *errStr = taos_errstr(res);
            snprintf(fetchErr, sizeof(fetchErr), "%s", errStr ? errStr : "");
        }
        taos_free_result(res);
        res = NULL;

        // this thread's whole range is handled, so a trailing stream error
        // cannot have cost us a name: do not retry and do not report a failure
        if (done >= thread->limit) {
            break;
        }

        if (fetchCode == TSDB_CODE_SUCCESS) {
            if (page == 0) {
                break;  // nothing left in this thread's range
            }
            continue;  // page shorter than requested: query the remainder
        }

        logError("fetch %s names failed(0x%08X %s) after %lld of %d handled: %s", what, fetchCode, fetchErr,
                 (long long)done, thread->limit, sql);
        code = fetchCode;
        if (!errorCodeCanRetry(fetchCode) || streamRetry >= retryCount || g_interrupted || g_fatalError) {
            goto cleanup;
        }
        streamRetry++;
        logInfo("retry fetching %s names: %s.%s, handled: %lld, attempt: %d", what,
                thread->dbInfo->dbName, thread->stbInfo->stbName, (long long)done, streamRetry);
        if (bckConnLevelError(fetchCode)) {
            resetConnectionPool();
            int connCode = TSDB_CODE_SUCCESS;
            *conn = getConnection(&connCode);
            if (*conn == NULL) {
                if (connCode != TSDB_CODE_SUCCESS) {
                    code = connCode;
                }
                goto cleanup;
            }
        }
        sleepMs(retrySleepMs);
    }

    if (doneOut) {
        *doneOut = done;
    }

cleanup:
    taosMemoryFree(sql);
    return code;
}

// Back up one child table, retrying transient (connection-level) failures.
// The original failure code is preserved: a successful reconnect must never
// turn a table that was not written into a success.
// `-k` is the number of retries after the first attempt (0 = single attempt).
static int backOneChildTableData(DataThread *thread, const char *childTableName) {
    int retryCount   = argRetryCount();
    int retrySleepMs = argRetrySleepMs();
    int attempts     = retryCount + 1;
    int code         = TSDB_CODE_SUCCESS;

    logDebug("backing up child table: %s.%s.%s", thread->dbInfo->dbName, thread->stbInfo->stbName, childTableName);

    for (int n = 0; n < attempts; n++) {
        code = backChildTableData(thread, childTableName);

        // check user cancelled / fatal server error
        if (g_interrupted || g_fatalError) {
            return g_fatalError ? g_fatalCode : TSDB_CODE_BCK_USER_CANCEL;
        }
        if (code == TSDB_CODE_SUCCESS) {
            // count completed CTB for progress display
            atomic_add_fetch_64(&g_progress.ctbDoneCur, 1);
            return TSDB_CODE_SUCCESS;
        }
        if (!errorCodeCanRetry(code)) {
            logError("backup child table data failed(0x%08X): %s.%s", code, thread->dbInfo->dbName, childTableName);
            return code;
        }

        // only a connection-level failure needs a rebuilt pool (with back-off
        // after a restart); transient server-side errors keep the healthy
        // connection and just retry
        if (bckConnLevelError(code)) {
            resetConnectionPool();
            int connCode = TSDB_CODE_SUCCESS;
            TAOS *conn = getConnection(&connCode);
            if (conn == NULL) {
                return (connCode != TSDB_CODE_SUCCESS) ? connCode : code;
            }
            thread->conn = conn;
        }
        logInfo("retry backup child table data: %s, times: %d", childTableName, n + 1);
        sleepMs(retrySleepMs);
    }

    logError("backup child table data failed after %d attempt(s)(0x%08X): %s.%s", attempts, code,
             thread->dbInfo->dbName, childTableName);
    return code;
}

//
// back data thread
//
static void* backDataThread(void *arg) {
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

    // Aggregate function on _c0 (the primary-key timestamp pseudo-column -
    // always the first column, regardless of its actual name):
    //   last(_c0)     – no time filter: benefits from last-value cache when enabled, O(n_tables)
    //   last_row(_c0) – with time filter: only inspects the last row in range, faster than full scan
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
    // child table names of this thread's range; backEnumerateObjects() appends
    // the LIMIT/OFFSET of the not-yet-handled remainder for every (re)try
    int sqlLen = (int)sizeof(innerWhere) + 512;
    char *sqlHead = (char *)taosMemoryMalloc(sqlLen);
    if (sqlHead == NULL) {
        taosMemoryFree(thread->writeBuf);
        thread->writeBuf = NULL;
        thread->code = TSDB_CODE_BCK_MALLOC_FAILED;
        return NULL;
    }
    snprintf(sqlHead, sqlLen,
             "SELECT tbname FROM ("
             "SELECT %s lts, tbname FROM `%s`.`%s` %s GROUP BY tbname"
             ") WHERE lts IS NOT NULL ORDER BY tbname",
             hasTf ? "last_row(_c0)" : "last(_c0)",
             thread->dbInfo->dbName,
             thread->stbInfo->stbName,
             innerWhere);

    TAOS *conn = getConnection(&thread->code);
    if (conn == NULL) {
        code = thread->code ? thread->code : TSDB_CODE_BCK_CONN_POOL_EXHAUSTED;
    } else {
        int64_t enumerated = 0;
        code = backEnumerateObjects(thread, &conn, sqlHead, "child table", backOneChildTableData, &enumerated);
        releaseConnection(conn);

        // A short list means part of the child tables could not be read (or the
        // database changed while backing up): never report such a backup as
        // successful, the missing tables would silently disappear from the dump.
        if (code == TSDB_CODE_SUCCESS && enumerated != thread->limit) {
            logError("child table enumeration incomplete for %s.%s: expected %d, got %lld",
                     thread->dbInfo->dbName, thread->stbInfo->stbName, thread->limit, (long long)enumerated);
            code = TSDB_CODE_BCK_INCOMPLETE_DATA;
        }
    }
    taosMemoryFree(sqlHead);

    // any failure aborts the run so the summary never reports a partial backup
    // as SUCCESS
    if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_BCK_USER_CANCEL && !g_interrupted) {
        g_fatalError = 1;
        g_fatalCode = code;
    }

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
    // Aggregate function on _c0 (the primary-key timestamp pseudo-column -
    // always the first column, regardless of its actual name):
    //   last(_c0)     – no time filter: benefits from last-value cache when enabled, O(n_tables)
    //   last_row(_c0) – with time filter: only inspects the last row in range, faster than full scan
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
             hasTf ? "last_row(_c0)" : "last(_c0)",
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
        if(taosThreadCreate(&threads[i].pid, NULL, backDataThread, (void *)&threads[i]) != 0) {
            logError("create backup thread failed(%s) for stb: %s.%s", strerror(errno), stbInfo->dbInfo->dbName, stbInfo->stbName);
            // Join already-started threads before freeing shared state
            for (int j = 0; j < i; j++) {
                taosThreadJoin(threads[j].pid, NULL);
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
        taosThreadJoin(threads[i].pid, NULL);
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
    // Percent-encode the normal table name (see backChildTableData).
    char encTb[TSDB_TABLE_NAME_LEN * 3 + 1] = {0};
    const char *encTbName = bckNameEncode(tableName, encTb, sizeof(encTb));
    if (encTbName == NULL) encTbName = tableName;
    char pathFile[MAX_PATH_LEN];
    snprintf(pathFile, sizeof(pathFile), "%s/%s.%s", dirPath, encTbName, ext);

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
        logError("generate backup table sql failed(0x%08X, %s): %s.%s", code, bckErrMsg(code), dbName, tableName);
        return code;
    }

    // write to .tmp first, then rename on success
    char tmpFile[MAX_PATH_LEN];
    snprintf(tmpFile, sizeof(tmpFile), "%s.tmp", pathFile);
    int64_t rows = 0;
    code = queryWriteBinary(thread->conn, sql, format, tmpFile, &rows, NULL);
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

// Back up one normal table, retrying transient (connection-level) failures.
// The original failure code is preserved: a successful reconnect must never
// turn a table that was not written into a success.
// `-k` is the number of retries after the first attempt (0 = single attempt).
static int backOneNormalTableData(DataThread *thread, const char *tableName) {
    int retryCount   = argRetryCount();
    int retrySleepMs = argRetrySleepMs();
    int attempts     = retryCount + 1;
    int code         = TSDB_CODE_SUCCESS;

    logDebug("backing up normal table: %s.%s", thread->dbInfo->dbName, tableName);

    for (int n = 0; n < attempts; n++) {
        code = backNormalOneTable(thread, tableName);

        if (g_interrupted || g_fatalError) {
            return g_fatalError ? g_fatalCode : TSDB_CODE_BCK_USER_CANCEL;
        }
        if (code == TSDB_CODE_SUCCESS) {
            // count completed NTB for progress display
            atomic_add_fetch_64(&g_progress.ctbDoneCur, 1);
            return TSDB_CODE_SUCCESS;
        }
        if (!errorCodeCanRetry(code)) {
            logError("backup normal table data failed(0x%08X): %s.%s", code, thread->dbInfo->dbName, tableName);
            return code;
        }

        // only a connection-level failure needs a rebuilt pool (with back-off
        // after a restart); transient server-side errors keep the healthy
        // connection and just retry
        if (bckConnLevelError(code)) {
            resetConnectionPool();
            int connCode = TSDB_CODE_SUCCESS;
            TAOS *conn = getConnection(&connCode);
            if (conn == NULL) {
                return (connCode != TSDB_CODE_SUCCESS) ? connCode : code;
            }
            thread->conn = conn;
        }
        logInfo("retry backup normal table data: %s, times: %d", tableName, n + 1);
        sleepMs(retrySleepMs);
    }

    logError("backup normal table data failed after %d attempt(s)(0x%08X): %s.%s", attempts, code,
             thread->dbInfo->dbName, tableName);
    return code;
}

// normal table data backup thread
static void* backNtbDataThread(void *arg) {
    DataThread *thread = (DataThread *)arg;
    const char *dbName = thread->dbInfo->dbName;
    logInfo("normal table data thread %d started for %s (offset=%d, limit=%d)", 
            thread->index, dbName, thread->offset, thread->limit);

    // normal table names of this thread's range; backEnumerateObjects() appends
    // the LIMIT/OFFSET of the not-yet-handled remainder for every (re)try
    char sqlHead[1024] = {0};
    char specFilter[512] = "";
    if (argSpecTables()) {
        char inClause[400] = "";
        argBuildInClause("table_name", inClause, sizeof(inClause));
        snprintf(specFilter, sizeof(specFilter), " AND %s", inClause);
    }
    char escDb[TSDB_DB_NAME_LEN * 2 + 1] = {0};
    const char *eDb = bckSqlEscape(dbName, escDb, sizeof(escDb));
    if (eDb == NULL) eDb = dbName;
    snprintf(sqlHead, sizeof(sqlHead), 
             "SELECT table_name FROM information_schema.ins_tables "
             "WHERE db_name='%s' AND stable_name IS NULL"
             " AND type NOT LIKE 'VIRTUAL%%'%s "
             "ORDER BY table_name",
             eDb, specFilter);

    TAOS *conn = getConnection(&thread->code);
    if (!conn) {
        return NULL;
    }

    int64_t enumerated = 0;
    int code = backEnumerateObjects(thread, &conn, sqlHead, "normal table", backOneNormalTableData, &enumerated);
    releaseConnection(conn);

    // A short list means part of the normal tables could not be read (or the
    // database changed while backing up): never report such a backup as success.
    if (code == TSDB_CODE_SUCCESS && enumerated != thread->limit) {
        logError("normal table enumeration incomplete for db %s: expected %d, got %lld", dbName, thread->limit,
                 (long long)enumerated);
        code = TSDB_CODE_BCK_INCOMPLETE_DATA;
    }

    // any failure aborts the run so the summary never reports a partial backup
    // as SUCCESS
    if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_BCK_USER_CANCEL && !g_interrupted) {
        g_fatalError = 1;
        g_fatalCode = code;
    }

    thread->code = code;
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
        if (taosThreadCreate(&threads[i].pid, NULL, backNtbDataThread, (void *)&threads[i]) != 0) {
            logError("create ntb backup thread failed: %s", strerror(errno));
            // Join already-started threads before freeing shared state
            for (int j = 0; j < i; j++) {
                taosThreadJoin(threads[j].pid, NULL);
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
        taosThreadJoin(threads[i].pid, NULL);
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
    //     if -C: skip this database entirely (already backed up);
    //     else: delete flag and run fresh (overwrite existing .dat files).
    //   - flag absent                  → previous run was interrupted;
    //     keep existing .dat files and resume from where we left off.
    char completeFlagPath[MAX_PATH_LEN];
    backCompleteFlagPath(dbName, completeFlagPath, sizeof(completeFlagPath));
    if (taosCheckExistFile(completeFlagPath)) {
        if (argCheckpoint()) {
            logInfo("skip database %s: already completed in previous.", dbName);
            return TSDB_CODE_SUCCESS;
        }
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
    // fresh (overwrite) rather than skip existing files. taosOpenFile can fail
    // on a transient local error (e.g. momentary fd/resource pressure right
    // after a large backup), so retry a few times before giving up - losing
    // the flag doesn't lose data (a later -C run just falls back to per-table
    // resume instead of whole-db skip), but it's cheap to make the common case
    // reliable rather than silently degrade on the first transient hiccup.
    if (code == TSDB_CODE_SUCCESS) {
        bool flagWritten = false;
        for (int attempt = 1; attempt <= 3 && !flagWritten; attempt++) {
            TdFilePtr fp = taosOpenFile(completeFlagPath, TD_FILE_WRITE | TD_FILE_CREATE | TD_FILE_TRUNC);
            if (fp) {
                taosCloseFile(&fp);
                flagWritten = true;
                logDebug("backup db %s: complete flag written (%s)", dbName, completeFlagPath);
            } else if (attempt < 3) {
                taosMsleep(100 * attempt);
            }
        }
        if (!flagWritten) {
            logWarn("backup db %s: failed to write complete flag after retries (%s)", dbName, completeFlagPath);
        }
    }

    return code;
}