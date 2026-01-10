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

//
// -------------------------------------- UTIL -----------------------------------------
//

void loadCheckPoint(StbInfo *stbInfo, DataThread *threads, int threadCnt) {
}

void saveCheckPoint(StbInfo *stbInfo, DataThread *thread, int offset) {
}

int genBackTableSql(const char *dbName, const char *tableName, char *sql, int len) {
    char *timeFilter = argTimeFilter();
    if (timeFilter != NULL) {
        // no time filter
        timeFilter = "";
    }
    
    snprintf(sql, len, "SELECT * FROM `%s`.`%s` %s", dbName, tableName, timeFilter);
    logDebug(" backup table sql: %s", sql);

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
    char fileName[MAX_PATH_LEN];
    code = obtainFileName(BACK_FILE_DATA, dbName, stbName, childTableName, 0, format, fileName, sizeof(fileName));
    if (code != TSDB_CODE_SUCCESS) {
        return code;
    }

    // query sql
    char sql[512] = {0};
    code = genBackTableSql(dbName, childTableName, sql, sizeof(sql));
    if (code != TSDB_CODE_SUCCESS) {
        logError("generate backup table sql failed(%d): %s.%s", code, dbName, childTableName);
        return code;
    }

    code = queryWriteBinary(thread->conn, sql, format, fileName);

    return code;
}

//
// back data thread
//
static void* backDataThread(void *arg) {
    int retryCount   = argRetryCount();
    int retrySleepMs = argRetrySleepMs();
    
    DataThread * thread = (DataThread *)arg;

    char sql[512] = {0};
    snprintf(sql, sizeof(sql), "select tbname from `%s`.`%s` ORDER BY tbname LIMIT %d OFFSET %d;",
             thread->dbInfo->dbName,
             thread->stbInfo->stbName,
             thread->limit,
             thread->offset);
    
    // query child table names
    TAOS* conn = getConnect();
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
    while ((row = taos_fetch_row(res))) {
        char *childTableName = (char *)row[0];
        if (childTableName == NULL) {
            logWarn("child table name is NULL, skip. offset: %d sql=%s", offset, sql);
            offset += 1;
            continue;
        }

        logInfo("backup child table data: %s", childTableName);
        
        // support retry
        int n = 0;
        while (n < retryCount) {
            // back child table data
            int code = backChildTableData(thread, childTableName);

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

        // save checkpoint
        offset += 1;
        saveCheckPoint(thread->stbInfo, thread, offset);
    }

    taos_free_result(res);
    releaseConnection(conn);

    return NULL;
}

//
// split child tables to thread groups
//
DataThread * splitDataThread(StbInfo *stbInfo, int *code, int *outCount) {
    int threadCnt = *outCount;
    DBInfo *dbInfo = stbInfo->dbInfo;
    const char* dbName = stbInfo->dbInfo->dbName;
    const char* stbName = stbInfo->stbName;

    char sql[512] = {0};
    snprintf(sql, sizeof(sql), "select count(*) from information_schema.ins_tables where db_name='%s' and stable_name='%s';", dbName, stbName);
    int tableCnt = 0;
    
    // query table count
    code = getFirstIntValue(sql, &tableCnt);
    if (code != TSDB_CODE_SUCCESS) {
        logError("get child table count failed(%d): %s.%s", code, dbName, stbName);
        return NULL;
    }
    if (tableCnt == 0) {
        logWarn("%s.%s child table count is zero.", dbName, stbName);
        *outCount = 0;
        return NULL;
    }

    if (tableCnt < threadCnt) {
        threadCnt = tableCnt;
    }

    // tag thread
    DataThread * threads = (DataThread *)calloc(threadCnt, sizeof(DataThread));
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

    // splite child tables to thread groups
    DataThread * threads = splitDataThread(stbInfo, &code, &count);
    if (threads == NULL) {
        return code;
    }
    logInfo("backup data thread count: %d", count);

    // load checkpoint
    loadCheckPoint(stbInfo, threads, count);

    // create threads
    for (int i = 0; i < count; i++) {
        if(pthread_create(&threads[i].pid, NULL, backDataThread, (void *)&threads[i]) != 0) {
            logError("create backup thread failed(%s) for stb: %s.%s", strerror(errno), stbInfo->dbInfo->dbName, stbInfo->stbName);
            free(threads);
            return TSDB_CODE_BCK_CREATE_THREAD_FAILED;
        }
    }

    // wait threads
    for (int i = 0; i < count; i++) {
        pthread_join(threads[i].pid, NULL);
    }

    // free
    free(threads);
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
    if (stbNames == NULL) {
        return code;
    }
    for (int i = 0; stbNames[i] != NULL; i++) {
        logInfo("backup super table meta: %s.%s\n", dbName, stbNames[i]);
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
    code = backNormalTableData(dbName);
    if (code != TSDB_CODE_SUCCESS) {
        return code;
    }

    return code;
}