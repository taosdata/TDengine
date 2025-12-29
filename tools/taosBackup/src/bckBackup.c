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
    
#include <pthread.h>
#include <string.h>
#include <errno.h>

#include "taos.h"
#include "tdef.h"
#include "taoserror.h"

#include "bckLog.h"
#include "bckError.h"
#include "bckArgs.h"
#include "util.h"
#include "bckBackup.h"
#include "taosBackup.h"


//
// -------------------------------------- UTIL -----------------------------------------
//

int genBackFileName(BackFileType fileType, const char *dbName, const char *tableName, char *fileName, int len) {
    return TSDB_CODE_SUCCESS;
}


int genBackTableSql(const char *dbName, const char *tableName, char *sql, int len) {
    snprintf(sql, len, "SELECT * FROM %s.%s", dbName, tableName);

    return TSDB_CODE_SUCCESS;
}


void obtainDBFile(const char *dbName,  char *dbPath, int len, char* fileName) {
    // db path: <outPath>/db_<crc>/
    snprintf(dbPath, len, "%s/db_%x/%s", argOutPath(), getCrc(dbName), fileName);
    return TSDB_CODE_SUCCESS;
}


//
// -------------------------------------- META -----------------------------------------
//
int backCreateDbSql(const char *dbName) {
    int code = TSDB_CODE_FAILED;
    
    // path
    char sqlFile[MAX_PATH_LEN] = {0};
    obtainDBFile(dbName, sqlFile, sizeof(sqlFile), FILE_DBSQL);

    // sql
    char sql[512] = {0};
    snprintf(sql, sizeof(sql), "show create database %s;", dbName);
    code = queryWriteFile(sql, sqlFile);

    return code;
}


int backCreateStbSql(const char *dbName, const char *stbName) {
    int code = TSDB_CODE_FAILED;
    
    // path
    char sqlFile[MAX_PATH_LEN] = {0};
    obtainDBFile(dbName, sqlFile, sizeof(sqlFile), FILE_DBSQL);

    // sql
    char sql[512] = {0};
    snprintf(sql, sizeof(sql), "show create table %s.%s;", dbName, stbName);
    code = queryWriteFile(sql, sqlFile);

    return code;
}

int backStbSchema(const char *dbName, const char *stbName, char ** selectTags) {
    int code = TSDB_CODE_FAILED;
    
    // path
    char jsonFile[MAX_PATH_LEN] = {0};
    char name[64] = {0};
    snprintf(name, sizeof(name), "stb_%x.json", getCrc(stbName));
    
    obtainDBFile(dbName, jsonFile, sizeof(jsonFile), name);
    
    // json
    char sql[512] = {0};
    snprintf(sql, sizeof(sql), "show create table %s.%s;", dbName, stbName);
    code = queryWriteJson(sql, jsonFile, selectTags);

    return code;
}

//
// back data thread
//
static void* backTagThread(void *arg) {
    return NULL;
}

//
// split tags with vgroups
//
TagThread ** splitTagWithVGroups(const char *dbName, const char *stbName, int *code, int *outCount) {
    *code = TSDB_CODE_SUCCESS;
    *outCount = 0;
    return NULL;
}

//
// tag create threads
//
int backChildTableTags(const char *dbName, const char *stbName) {
    int code = TSDB_CODE_FAILED;
    int count = 0;

    // splite tags with vgroups
    TagThread ** threads = splitTagWithVGroups(dbName, stbName, &code, &count);
    if (threads == NULL) {
        return code;
    }

    // create threads
    for (int i = 0; i < count; i++) {
        if(pthread_create(&threads[i]->pid, NULL, backTagThread, (void *)threads[i]) != 0) {
            logError("create backup thread failed(%s) for stb: %s.%s", strerror(errno), dbName, stbName);
            free(threads);
            return TSDB_CODE_BACKUP_CREATE_THREAD_FAILED;
        }
    }

    // wait threads
    for (int i = 0; i < count; i++) {
        pthread_join(threads[i]->pid, NULL);
    }

    // free
    free(threads);
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
int backDatabaseMeta(const char *dbName) {
    int code = TSDB_CODE_FAILED;

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

    for (int i = 0; stbNames[i] != NULL; i++) {
        printf("backup super table meta: %s.%s\n", dbName, stbNames[i]);

        // super tables

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
            if(selectTags) {
                free(selectTags);
            }            
            return code;
        }


        code = backChildTableTags(dbName, stbNames[i], selectTags);
        if (code != TSDB_CODE_SUCCESS) {
            freeArrayPtr(stbNames);
            if(selectTags) {
                free(selectTags);

            return code;
        }
        if(selectTags) {
            free(selectTags);
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



//
// -------------------------------------- DATA -----------------------------------------
//


//
// write data block to file
//
int writeBlockToFile(const char *fileName, void *block) {
    int code = TSDB_CODE_FAILED;

    return code;
}

//
// back child table data on thread group
//
int backChildTableData(DataThread* group, const char *childTableName) {
    int code = TSDB_CODE_FAILED;
    // get write file name
    char fileName[MAX_PATH_LEN];
    code = genBackFileName(BACK_FILE_TYPE_DATA, group->dbName, childTableName, fileName, sizeof(fileName));
    if (code != TSDB_CODE_SUCCESS) {
        return code;
    }

    // query sql
    char sql[512] = {0};
    code = genBackTableSql(group->dbName, childTableName, sql, sizeof(sql));
    if (code != TSDB_CODE_SUCCESS) {
        logError("generate backup table sql failed(%d): %s.%s", code, group->dbName, childTableName);
        return code;
    }

    TAOS_RES* res = taos_query(group->conn, sql);
    if (res == NULL) {
        logError("query child table data failed(%s): %s", taos_errstr(res), sql);
        return taos_errno(res);
    }

    // while fetch data
    int numRows = 0;
    int blockRows = 0;
    void *block = NULL;
    while (taos_fetch_raw_block(res, &blockRows, &block) == TSDB_CODE_SUCCESS) {
        // write to file
        code = writeBlockToFile(fileName, block);
        if (code != TSDB_CODE_SUCCESS) {
            logError("write data block to file failed(%d): %s", code, fileName);
            taos_free_result(res);
            return code;
        }
        numRows += blockRows;
    }

    // free
    taos_free_result(res);

    return code;
}

//
// back data thread
//
static void* backDataThread(void *arg) {
    int retryCount   = argRetryCount();
    int retrySleepMs = argRetrySleepMs();

    DataThread * group = (DataThread *)arg;
    for (int i = 0; i < group->numChildTables; i++) {
        logInfo("backup child table data: %s", group->childTableNames[i]);
        
        // support retry
        int n = 0;
        while (n < retryCount) {
            // back child table data
            int code = backChildTableData(group, group->childTableNames[i]);

            // check code
            if (code == TSDB_CODE_SUCCESS) {
                // success
                break;
            }
            else if (errorCodeCanRetry(code)) {
                // can retry
                n += 1;
                logInfo("retry backup child table data: %s, times: %d", group->childTableNames[i], n);
                sleepMs(retrySleepMs);
            } else {
                // not retry
                logError("backup child table data failed(%d): %s.%s", code, group->dbName, group->childTableNames[i]);
                break;
            }
        }
    }

    return NULL;
}

//
// split child tables to thread groups
//
DataThread ** splitTablesToThread(const char *dbName, const char *stbName, int *code, int threadCount) {
    *code = TSDB_CODE_SUCCESS;
    return NULL;
}

//
// data create threads
//
int backStbData(const char *dbName, const char *stbName) {
    int code = TSDB_CODE_FAILED;
    int count = argDataThread();

    // splite child tables to thread groups
    DataThread ** threads = splitTablesToThread(dbName, stbName, &code, count);
    if (threads == NULL) {
        return code;
    }

    // create threads
    for (int i = 0; i < count; i++) {
        if(pthread_create(&threads[i]->pid, NULL, backDataThread, (void *)threads[i]) != 0) {
            logError("create backup thread failed(%s) for stb: %s.%s", strerror(errno), dbName, stbName);
            free(threads);
            return TSDB_CODE_BACKUP_CREATE_THREAD_FAILED;
        }
    }

    // wait threads
    for (int i = 0; i < count; i++) {
        pthread_join(threads[i]->pid, NULL);
    }

    // free
    free(threads);
    return code;
}

//
// backup database data
//
int backDatabaseData(const char *dbName) {
    int code = TSDB_CODE_FAILED;

    //
    // super tables
    // 
    char ** stbNames = getDBSuperTableNames(dbName, &code);
    if (stbNames == NULL) {
        return code;
    }
    for (int i = 0; stbNames[i] != NULL; i++) {
        code = backStbData(dbName, stbNames[i]);
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

//
// ------------------- main ---------------------
//

//
// backup database
//
int backDatabase(const char *dbName) {
    // meta
    int code = TSDB_CODE_FAILED;
    code = backDatabaseMeta(dbName);
    if (code != TSDB_CODE_SUCCESS) {
        printf("backup database meta failed, code: %d\n", code);
        return code;
    }

    // data
    code = backDatabaseData(dbName);
    if (code != TSDB_CODE_SUCCESS) {
        printf("backup super table meta failed, code: %d\n", code);
        return code;
    }

    return code;
}

//
// backup main function
//
int backupMain() {
    // init
    int code = TSDB_CODE_FAILED;

    char **backDB = argsGetBackDB();
    if (backDB == NULL) {
        printf("no database to backup\n");
        return TSDB_CODE_BACKUP_INVALID_PARAM;
    }

    for (int i = 0; backDB[i] != NULL; i++) {
        printf("backup database: %s\n", backDB[i]);

        // backup data
        code = backupDatabase(backDB[i]);
        if (code != TSDB_CODE_SUCCESS) {
            printf("backup data failed, code: %d\n", code);
            return code;
        }
    }

    return code;
}