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


//
// ----------------------------------- FUNCTION --------------------------------------
//

int genBackTableSql(const char *dbName, const char *tableName, char *sql, int len) {
    snprintf(sql, len, "SELECT * FROM %s.%s", dbName, tableName);

    return TSDB_CODE_SUCCESS;
}



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
    snprintf(sql, sizeof(sql), "show create database %s;", dbName);
    code = queryWriteTxt(sql, sqlFile);

    return code;
}


int backCreateStbSql(const char *dbName, const char *stbName) {
    int code = TSDB_CODE_FAILED;
    
    // path
    char sqlFile[MAX_PATH_LEN] = {0};
    obtainFileName(BACK_FILE_DBSQL, dbName, NULL, NULL, 0, 0, BINARY_TAOS, sqlFile, sizeof(sqlFile));

    // sql
    char sql[512] = {0};
    snprintf(sql, sizeof(sql), "show create table %s.%s;", dbName, stbName);
    code = queryWriteTxt(sql, sqlFile);

    return code;
}

int backStbSchema(const char *dbName, const char *stbName, char ** selectTags) {
    int code = TSDB_CODE_FAILED;
    
    // path
    char jsonFile[MAX_PATH_LEN] = {0};    
    obtainFileName(BACK_FILE_STBJSON, dbName, stbName, NULL, 0, 0, BINARY_TAOS, jsonFile, sizeof(jsonFile));
    
    // json
    char sql[512] = {0};
    snprintf(sql, sizeof(sql), "show create table %s.%s;", dbName, stbName);
    code = queryWriteJson(sql, jsonFile, selectTags);

    return code;
}

//
// back tag data thread
//
static void* backTagThread(void *arg) {
    TagThread *thread = (TagThread *)arg;

    char *sql = (char *)calloc(TSDB_MAX_SQL_LEN, sizeof(char));
    if (sql == NULL) {
        logError("allocate sql buffer failed");
        return NULL;
    }
    snprintf(sql, TSDB_MAX_SQL_LEN, 
             "SELECT DISTINCT tbname,%s FROM %s.%s "
             "ORDER BY tbname "
             "LIMIT %d OFFSET %d;",
             thread->stbInfo->selectTags,
             thread->dbInfo->dbName,
             thread->stbInfo->stbName,
             thread->limit,
             thread->offset);
    
    // sql result to file
    char fileName[MAX_PATH_LEN] = {0};
    StorageFormat format = argStorageFormat();

    int code = obtainFileName(BACK_FILE_TAG, thread->dbInfo->dbName, thread->stbInfo->stbName, NULL, thread->index, 0, format, fileName, sizeof(fileName));
    if (code != TSDB_CODE_SUCCESS) {
        logError("generate backup file name failed(%d): %s.%s", code, thread->dbInfo->dbName, thread->stbInfo->stbName);
        freePtr(sql);
        return NULL;
    }
    freePtr(sql);

    code = queryWriteBinary(sql, format, fileName);
    if (code != TSDB_CODE_SUCCESS) {
        logError("query write binary failed. sql=%s, format=%d file=%s", sql, format, fileName);
        return NULL;
    }
    
    return NULL;
}

//
// split tag thread
//
TagThread * splitTagThread(DBInfo *dbInfo, StbInfo *stbInfo, int *code, int *outCount) {
    int threadCnt = *outCount;
    const char* dbName = dbInfo->dbName;
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
    TagThread * threads = (TagThread *)calloc(threadCnt, sizeof(TagThread));
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
// tag create threads
//
int backChildTableTags(DBInfo *dbInfo, StbInfo *stbInfo) {
    int code = TSDB_CODE_FAILED;
    int count = argTagThread();

    // splite tags with vgroups
    TagThread *threads = splitTagThread(dbInfo, stbInfo, &code, &count);
    if (threads == NULL) {
        return code;
    }

    // create threads
    for (int i = 0; i < count; i++) {
        if(pthread_create(&threads[i].pid, NULL, backTagThread, (void *)&threads[i]) != 0) {
            logError("create backup thread failed(%s) for stb:%s", strerror(errno), stbInfo->stbName);
            freePtr(threads);
            return TSDB_CODE_BACKUP_CREATE_THREAD_FAILED;
        }
    }

    // wait threads
    for (int i = 0; i < count; i++) {
        pthread_join(threads[i].pid, NULL);
    }

    // free
    freePtr(threads);
    return code;
}

//
// normal tables create sql
//
int backNormalTablesSql(const char *dbName) {
    int code = TSDB_CODE_FAILED;

    return code;
}

//
// backup database meta
//
int backDatabaseMeta(DBInfo *dbInfo) {
    int code = TSDB_CODE_FAILED;
    char *dbName = dbInfo->dbName;

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
    int code = TSDB_CODE_FAILED;
    char ** stbNames = getDBSuperTableNames(dbName, &code);
    if (stbNames == NULL) {
        return code;
    }

    // Loop super table
    for (int i = 0; stbNames[i] != NULL; i++) {
        logInfo("backup super table meta: %s.%s\n", dbName, stbNames[i]);
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

    //
    // normal tables sql
    //
    code = backNormalTablesSql(dbName);
    if (code != TSDB_CODE_SUCCESS) {
        return code;
    }

    return code;
}