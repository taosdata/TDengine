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
    
#include "backupMeta.h"
#include "bckPool.h"
#include "bckDb.h"


//
// ----------------------------------- FUNCTION --------------------------------------
//




//
// -------------------------------------- META -----------------------------------------
//
int backCreateDbSql(const char *dbName) {
    int code = TSDB_CODE_FAILED;
    
    // path
    char sqlFile[MAX_PATH_LEN] = {0};
    obtainFileName(BACK_FILE_DBSQL, dbName, NULL, NULL, 0, 0, BINARY_TAOS, sqlFile, sizeof(sqlFile));

    // sql
    char sql[512] = {0};
    snprintf(sql, sizeof(sql), "show create database `%s`;", dbName);
    code = queryWriteTxt(sql, 1, sqlFile);

    return code;
}

int backCreateStbSql(const char *dbName, const char *stbName) {
    int code = TSDB_CODE_FAILED;
    
    // path
    char sqlFile[MAX_PATH_LEN] = {0};
    obtainFileName(BACK_FILE_STBSQL, dbName, NULL, NULL, 0, 0, BINARY_TAOS, sqlFile, sizeof(sqlFile));

    // sql
    char sql[512] = {0};
    snprintf(sql, sizeof(sql), "show create table `%s`.`%s`;", dbName, stbName);
    code = queryWriteTxt(sql, 1, sqlFile);

    return code;
}

int backStbSchema(const char *dbName, const char *stbName, char ** selectTags) {
    int code = TSDB_CODE_FAILED;
    
    // path
    char csvFile[MAX_PATH_LEN] = {0};    
    obtainFileName(BACK_FILE_STBCSV, dbName, stbName, NULL, 0, 0, BINARY_TAOS, csvFile, sizeof(csvFile));
    
    // csv
    char sql[512] = {0};
    snprintf(sql, sizeof(sql), "describe `%s`.`%s`;", dbName, stbName);
    code = queryWriteCsv(sql, csvFile, selectTags);

    return code;
}

//
// back tag data thread
//
static void* backTagThread(void *arg) {
    TagThread *thread = (TagThread *)arg;
    thread->code = TSDB_CODE_SUCCESS;

    char *sql = (char *)taosMemoryCalloc(TSDB_MAX_SQL_LEN, sizeof(char));
    if (sql == NULL) {
        logError("allocate sql buffer failed");
        thread->code = TSDB_CODE_BCK_MALLOC_FAILED;
        return NULL;
    }
    // build WHERE clause for specific tables if requested
    char whereClause[4096] = "";
    if (argSpecTables()) {
        char inClause[3800] = "";
        argBuildInClause("tbname", inClause, sizeof(inClause));
        snprintf(whereClause, sizeof(whereClause), "WHERE %s ", inClause);
    }
    snprintf(sql, TSDB_MAX_SQL_LEN, 
             "SELECT DISTINCT tbname,%s FROM `%s`.`%s` "
             "%sORDER BY tbname "
             "LIMIT %d OFFSET %d;",
             thread->stbInfo->selectTags,
             thread->dbInfo->dbName,
             thread->stbInfo->stbName,
             whereClause,
             thread->limit,
             thread->offset);
    logDebug("backing up tags for thread %d: %s", thread->index, sql);         
    
    // sql result to file
    char fileName[MAX_PATH_LEN] = {0};
    StorageFormat format = argStorageFormat();

    int code = obtainFileName(BACK_FILE_TAG, thread->dbInfo->dbName, thread->stbInfo->stbName, NULL, thread->index, 0, format, fileName, sizeof(fileName));
    if (code != TSDB_CODE_SUCCESS) {
        logError("generate backup file name failed(%d): %s.%s", code, thread->dbInfo->dbName, thread->stbInfo->stbName);
        thread->code = code;
        freePtr(sql);
        return NULL;
    }

    code = queryWriteBinary(thread->conn, sql, format, fileName, NULL);
    if (code != TSDB_CODE_SUCCESS) {
        logError("query write binary failed. sql=%s, format=%d file=%s", sql, format, fileName);
    }
    thread->code = code;

    freePtr(sql);
    logInfo("backup tag thread %d finished.", thread->index);
    return NULL;
}

//
// split backup tag task
//
TagThread * splitTaskTag(DBInfo *dbInfo, StbInfo *stbInfo, int *code, int *outCount) {
    int threadCnt = *outCount;
    const char* dbName = dbInfo->dbName;
    const char* stbName = stbInfo->stbName;

    char sql[8192] = {0};
    char specFilter[4096] = "";
    if (argSpecTables()) {
        char inClause[3800] = "";
        argBuildInClause("table_name", inClause, sizeof(inClause));
        snprintf(specFilter, sizeof(specFilter), " AND %s", inClause);
    }
    snprintf(sql, sizeof(sql), "select count(*) from information_schema.ins_tables where db_name='%s' and stable_name='%s'%s;", dbName, stbName, specFilter);
    int tableCnt = 0;
    
    // query table count
    *code = queryValueInt(sql, 0, &tableCnt);
    if (*code != TSDB_CODE_SUCCESS) {
        logError("query table count failed(%d): %s", code, sql);
        return NULL;
    }
    // zero
    if (tableCnt == 0) {
        logDebug("no child table found for super table: %s.%s", dbName, stbName);
        *code = TSDB_CODE_SUCCESS;
        *outCount = 0;
        return NULL;
    }

    if (tableCnt < threadCnt) {
        threadCnt = tableCnt;
    }

    // tag thread
    TagThread * threads = (TagThread *)taosMemoryCalloc(threadCnt, sizeof(TagThread));
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
// tag create threads
//
int backChildTableTags(DBInfo *dbInfo, StbInfo *stbInfo) {
    int code = TSDB_CODE_FAILED;
    int count = argTagThread();

    // splite tags with vgroups
    TagThread *threads = splitTaskTag(dbInfo, stbInfo, &code, &count);
    if (threads == NULL) {
        return code;
    }

    // calculate total child tables
    int totalChildTables = 0;
    for (int i = 0; i < count; i++) {
        totalChildTables += threads[i].limit;
    }
    logInfo("backing up tags for %d child tables of %s.%s using %d tag threads", 
            totalChildTables, dbInfo->dbName, stbInfo->stbName, count);

    // create threads
    for (int i = 0; i < count; i++) {
        if(pthread_create(&threads[i].pid, NULL, backTagThread, (void *)&threads[i]) != 0) {
            logError("create backup thread failed(%s) for stb:%s", strerror(errno), stbInfo->stbName);
            freePtr(threads);
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
    freePtr(threads);
    return code;
}

//
// normal tables create sql
//
int backNormalTablesSql(const char *dbName) {
    int code = TSDB_CODE_SUCCESS;

    // get normal table names
    char **ntbNames = getDBNormalTableNames(dbName, &code);
    if (ntbNames == NULL) {
        if (code != TSDB_CODE_SUCCESS) return code;
        return TSDB_CODE_SUCCESS; // no normal tables
    }

    // count
    int ntbCount = 0;
    for (int i = 0; ntbNames[i] != NULL; i++) ntbCount++;
    if (ntbCount == 0) {
        freeArrayPtr(ntbNames);
        return TSDB_CODE_SUCCESS;
    }

    logInfo("backup %d normal table(s) sql for db: %s", ntbCount, dbName);
    atomic_add_fetch_64(&g_stats.ntbTotal, ntbCount);

    // open ntb.sql file
    char ntbSqlFile[MAX_PATH_LEN] = {0};
    obtainFileName(BACK_FILE_NTBSQL, dbName, NULL, NULL, 0, 0, BINARY_TAOS, ntbSqlFile, sizeof(ntbSqlFile));

    TdFilePtr fp = taosOpenFile(ntbSqlFile, TD_FILE_WRITE | TD_FILE_CREATE | TD_FILE_TRUNC);
    if (!fp) {
        logError("open ntb.sql failed: %s", ntbSqlFile);
        freeArrayPtr(ntbNames);
        return TSDB_CODE_BCK_CREATE_FILE_FAILED;
    }

    // for each normal table: show create table -> write to file
    TAOS *conn = getConnection();
    if (!conn) {
        taosCloseFile(&fp);
        freeArrayPtr(ntbNames);
        return TSDB_CODE_BCK_CONN_POOL_EXHAUSTED;
    }

    code = TSDB_CODE_SUCCESS;
    char sql[512];
    for (int i = 0; ntbNames[i] != NULL; i++) {
        if(g_interrupted) {
            code = TSDB_CODE_BCK_USER_CANCEL;
            break;
        }

        snprintf(sql, sizeof(sql), "show create table `%s`.`%s`;", dbName, ntbNames[i]);
        TAOS_RES *res = taos_query(conn, sql);
        code = taos_errno(res);
        if (!res || code != TSDB_CODE_SUCCESS) {
            logError("show create table failed(%s): %s", taos_errstr(res), sql);
            if (res) taos_free_result(res);
            break;
        }

        TAOS_ROW row = taos_fetch_row(res);
        if (row && row[1]) {
            int32_t *lens = taos_fetch_lengths(res);
            taosWriteFile(fp, row[1], lens[1]);
            taosWriteFile(fp, "\n", 1);
        }
        taos_free_result(res);
    }

    releaseConnection(conn);
    taosCloseFile(&fp);
    freeArrayPtr(ntbNames);
    return code;
}

//
// backup database meta
//
int backDatabaseMeta(DBInfo *dbInfo) {
    int code = TSDB_CODE_FAILED;
    const char *dbName = dbInfo->dbName;

    //
    // database sql
    //
    code = backCreateDbSql(dbName);
    if (code != TSDB_CODE_SUCCESS) {
        return code;
    }

    //
    // super tables meta
    //
    char ** stbNames = getDBSuperTableNames(dbName, &code);
    if (stbNames == NULL && code != TSDB_CODE_SUCCESS) {
        return code;
    }

    // Loop super table
    for (int i = 0; stbNames != NULL && stbNames[i] != NULL; i++) {
        if (g_interrupted) {
            code = TSDB_CODE_BCK_USER_CANCEL;
            break;
        }
        logInfo("backup super table meta: %s.%s\n", dbName, stbNames[i]);
        atomic_add_fetch_64(&g_stats.stbTotal, 1);
        StbInfo stbInfo;
        memset(&stbInfo, 0, sizeof(StbInfo));
        stbInfo.dbInfo = dbInfo;
        stbInfo.stbName = stbNames[i];

        // sql
        code = backCreateStbSql(dbName, stbNames[i]);
        if (code != TSDB_CODE_SUCCESS) {
            freeArrayPtr(stbNames);
            return code;
        }
        // schema
        char * selectTags = NULL; 
        code = backStbSchema(dbName, stbNames[i], &selectTags);
        if (code != TSDB_CODE_SUCCESS) {
            freeArrayPtr(stbNames);
            freePtr(selectTags);
            return code;
        }
        // tags
        stbInfo.selectTags = selectTags;
        code = backChildTableTags(dbInfo, &stbInfo);
        if (code != TSDB_CODE_SUCCESS) {
            freeArrayPtr(stbNames);
            freePtr(selectTags);
            
            return code;
        }
        // free
        freePtr(selectTags);
    }    

    freeArrayPtr(stbNames);

    if (code != TSDB_CODE_SUCCESS) {
        return code;
    }

    //
    // normal tables sql
    //
    code = backNormalTablesSql(dbName);
    if (code != TSDB_CODE_SUCCESS) {
        return code;
    }

    return code;
}