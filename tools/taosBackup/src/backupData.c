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
#include "storageTaos.h"
#include "storageParquet.h"
#include "bckPool.h"
#include "bckDb.h"

volatile int64_t g_backDataFiles = 0;

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
    // get write file name
    StorageFormat format = argStorageFormat();
    // ensure dir exist
    char dirPath[MAX_PATH_LEN];
    code = obtainFileName(BACK_DIR_DATA, 
                          dbName, 
                          stbName, 
                          childTableName, 
                          thread->index, 
                          g_backDataFiles, 
                          format, 
                          dirPath, 
                          sizeof(dirPath));
    if(!taosDirExist(dirPath)) {
        taosMkDir(dirPath);
    }

    // file
    char pathFile[MAX_PATH_LEN];
    code = obtainFileName(BACK_FILE_DATA, 
                          dbName, 
                          stbName, 
                          childTableName, 
                          thread->index, 
                          g_backDataFiles, 
                          format, 
                          pathFile, 
                          sizeof(pathFile));
    if (code != TSDB_CODE_SUCCESS) {
        return code;
    }

    // global file count
    atomic_add_fetch_64(&g_backDataFiles, 1);

    // skip if already completed (resume support)
    if (taosCheckExistFile(pathFile)) {
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
    code = queryWriteBinary(thread->conn, sql, format, tmpFile, &rows);
    if (code == TSDB_CODE_SUCCESS) {
        if (rename(tmpFile, pathFile) != 0) {
            logError("rename tmp file failed: %s -> %s, errno: %d", tmpFile, pathFile, errno);
            code = TSDB_CODE_FAILED;
        } else {
            atomic_add_fetch_64(&g_stats.totalRows, rows);
            if (rows > 0) {
                atomic_add_fetch_64(&g_stats.childTablesTotal, 1);
            }
        }
    } else {
        // remove incomplete tmp file
        remove(tmpFile);
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
    
    DataThread * thread = (DataThread *)arg;
    logInfo("data thread %d started for %s.%s (offset=%d, limit=%d)", 
            thread->index, thread->dbInfo->dbName, thread->stbInfo->stbName, thread->offset, thread->limit);

    char sql[512] = {0};
    snprintf(sql, sizeof(sql), "select DISTINCT tbname from `%s`.`%s` ORDER BY tbname LIMIT %d OFFSET %d;",
             thread->dbInfo->dbName,
             thread->stbInfo->stbName,
             thread->limit,
             thread->offset);
    
    // query child table names
    TAOS* conn = getConnection();
    if (!conn) {
        thread->code = g_interrupted ? TSDB_CODE_BCK_USER_CANCEL : TSDB_CODE_BCK_CONN_POOL_EXHAUSTED;
        return NULL;
    }
    TAOS_RES *res = taos_query(conn, sql);
    if (res == NULL || taos_errno(res)) {
        logError("query child table names failed(%s): %s", taos_errstr(res), sql);
        if (res) taos_free_result(res);
        releaseConnection(conn);
        return NULL;
    }

    // loop child tables
    TAOS_ROW row;
    int offset = thread->offset;
    char childTableName[TSDB_TABLE_NAME_LEN]= {0};
    int32_t code = TSDB_CODE_SUCCESS;
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
                // can retry
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

        // save checkpoint
        offset += 1;
    }

    taos_free_result(res);
    releaseConnection(conn);

    thread->code = code;
    logInfo("data thread %d finished for %s.%s", thread->index, thread->dbInfo->dbName, thread->stbInfo->stbName);
    return NULL;
}

//
// split child tables to thread groups
//
DataThread * splitTaskData(StbInfo *stbInfo, int *code, int *outCount) {
    int threadCnt = *outCount;
    DBInfo *dbInfo = stbInfo->dbInfo;
    const char* dbName = stbInfo->dbInfo->dbName;
    const char* stbName = stbInfo->stbName;

    // query table count
    char sql[512] = {0};
    snprintf(sql, sizeof(sql), "select count(*) from information_schema.ins_tables where db_name='%s' and stable_name='%s';", dbName, stbName);
    int32_t tableCnt = 0;
    *code = queryValueInt(sql, 0, &tableCnt);
    if (*code != TSDB_CODE_SUCCESS) {
        return NULL;
    }
    
    if (tableCnt == 0) {
        logDebug("%s.%s child table count is zero.", dbName, stbName);
        *code = TSDB_CODE_SUCCESS;
        *outCount = 0;
        return NULL;
    }

    if (tableCnt < threadCnt) {
        threadCnt = tableCnt;
    }

    // tag thread
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
        threads[i].conn    = getConnection();
        if (!threads[i].conn) {
            // release already-allocated connections
            for (int j = 0; j < i; j++) {
                releaseConnection(threads[j].conn);
            }
            taosMemoryFree(threads);
            *code = g_interrupted ? TSDB_CODE_BCK_USER_CANCEL : TSDB_CODE_BCK_CONN_POOL_EXHAUSTED;
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

    // splite child tables to thread groups
    DataThread * threads = splitTaskData(stbInfo, &code, &count);
    if (threads == NULL) {
        return code;
    }
    
    // calculate total child tables
    int totalChildTables = 0;
    for (int i = 0; i < count; i++) {
        totalChildTables += threads[i].limit;
    }
    logInfo("backing up %d child tables for %s.%s using %d data threads", 
            totalChildTables, stbInfo->dbInfo->dbName, stbInfo->stbName, count);

    // create threads
    for (int i = 0; i < count; i++) {
        if(pthread_create(&threads[i].pid, NULL, backDataThread, (void *)&threads[i]) != 0) {
            logError("create backup thread failed(%s) for stb: %s.%s", strerror(errno), stbInfo->dbInfo->dbName, stbInfo->stbName);
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

    // skip if already completed (resume support)
    if (taosCheckExistFile(pathFile)) {
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
        if (rename(tmpFile, pathFile) != 0) {
            logError("rename tmp file failed: %s -> %s, errno: %d", tmpFile, pathFile, errno);
            code = TSDB_CODE_FAILED;
        } else {
            atomic_add_fetch_64(&g_stats.totalRows, rows);
        }
    } else {
        remove(tmpFile);
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
    char sql[512] = {0};
    snprintf(sql, sizeof(sql), 
             "SELECT table_name FROM information_schema.ins_tables "
             "WHERE db_name='%s' AND stable_name IS NULL "
             "ORDER BY table_name LIMIT %d OFFSET %d;",
             dbName, thread->limit, thread->offset);

    TAOS *conn = getConnection();
    if (!conn) {
        thread->code = g_interrupted ? TSDB_CODE_BCK_USER_CANCEL : TSDB_CODE_BCK_CONN_POOL_EXHAUSTED;
        return NULL;
    }
    TAOS_RES *res = taos_query(conn, sql);
    if (res == NULL || taos_errno(res)) {
        logError("query normal table names failed(%s): %s", taos_errstr(res), sql);
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
                break;
            } else if (errorCodeCanRetry(thread->code)) {
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
        logInfo("no normal tables in db: %s", dbName);
        return TSDB_CODE_SUCCESS;
    }

    logInfo("backup %d normal table(s) data for db: %s", tableCnt, dbName);

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
    ntbStbInfo.stbName = "_ntb";

    for (int i = 0; i < threadCnt; i++) {
        threads[i].dbInfo  = dbInfo;
        threads[i].stbInfo = &ntbStbInfo;
        threads[i].index   = i + 1;
        threads[i].limit   = base + (remain > 0 ? 1 : 0);
        if (remain > 0) remain--;
        threads[i].offset  = offset;
        threads[i].conn    = getConnection();
        if (!threads[i].conn) {
            for (int j = 0; j < i; j++) {
                releaseConnection(threads[j].conn);
            }
            taosMemoryFree(threads);
            return g_interrupted ? TSDB_CODE_BCK_USER_CANCEL : TSDB_CODE_BCK_CONN_POOL_EXHAUSTED;
        }
        offset += threads[i].limit;
    }

    for (int i = 0; i < threadCnt; i++) {
        if (pthread_create(&threads[i].pid, NULL, backNtbDataThread, (void *)&threads[i]) != 0) {
            logError("create ntb backup thread failed: %s", strerror(errno));
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

    //
    // super tables
    // 
    char ** stbNames = getDBSuperTableNames(dbName, &code);
    if (stbNames == NULL && code != TSDB_CODE_SUCCESS) {
        return code;
    }
    for (int i = 0; stbNames != NULL && stbNames[i] != NULL; i++) {
        logInfo("backup super table: %s.%s\n", dbName, stbNames[i]);
        StbInfo stbInfo;
        memset(&stbInfo, 0, sizeof(StbInfo));
        stbInfo.dbInfo = dbInfo;
        stbInfo.stbName = stbNames[i];

        code = backStbData(&stbInfo);
        if (code != TSDB_CODE_SUCCESS) {
            freeArrayPtr(stbNames);
            return code;
        }
    }

    freeArrayPtr(stbNames);

    //
    // normal tables
    //
    code = backNormalTableData(dbInfo);
    if (code != TSDB_CODE_SUCCESS) {
        return code;
    }

    return code;
}