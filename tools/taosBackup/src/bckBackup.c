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

#include "back.h"
#include "bckLog.h"
#include "bckError.h"
#include "bckArgs.h"
#include "util.h"
#include "bckBackup.h"


//
// ---------------- util ----------------
//

int genBackFileName(BackFileType fileType, const char *dbName, const char *tableName, char *fileName, int len) {
    return TSDB_CODE_SUCCESS;
}


int genBackTableSql(const char *dbName, const char *tableName, char *sql, int len) {
    snprintf(sql, len, "SELECT * FROM %s.%s", dbName, tableName);

    return TSDB_CODE_SUCCESS;
}


//
// ------------------- meta ---------------------
//

int backCreateDbSql(const char *dbName) {
    int code = TSDB_CODE_FAILED;

    return code;
}


int backCreateStbSql(const char *dbName, const char *stbName) {
    int code = TSDB_CODE_FAILED;

    return code;
}

int backChildTableTags(const char *dbName, const char *stbName) {
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
        code = backCreateStbSql(dbName, stbNames[i]);
        if (code != TSDB_CODE_SUCCESS) {
            freeArrayPtr(stbNames);
            return code;
        }

        code = backChildTableTags(dbName, stbNames[i]);
        if (code != TSDB_CODE_SUCCESS) {
            freeArrayPtr(stbNames);
            return code;
        }
    }    

    freeArrayPtr(stbNames);

    //
    // normal tables sql
    //
    code = backCreateNormalTablesSql(dbName);
    if (code != TSDB_CODE_SUCCESS) {
        return code;
    }

    return code;
}

//
// ------------------- data ---------------------
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
int backChildTableData(GroupThread* group, const char *childTableName) {
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
// back thread
//
static void* backGroupThread(void *arg) {
    int retryCount   = argRetryCount();
    int retrySleepMs = argRetrySleepMs();

    GroupThread * group = (GroupThread *)arg;
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
// back stb data
//
int backStbData(const char *dbName, const char *stbName) {
    int code = TSDB_CODE_FAILED;
    int count = 0;

    // splite child tables to thread groups
    GroupThread ** groups = splitChildTablesToThreadGroups(dbName, stbName, &code, &count);
    if (groups == NULL) {
        return code;
    }
    pthread_t *pids = calloc(1, count * sizeof(pthread_t));

    // create threads
    for (int i = 0; i < count; i++) {
        if(pthread_create(pids[i], NULL, backGroupThread, (void *)groups[i]) != 0) {
            logError("create backup thread failed(%s) for stb: %s.%s", strerror(errno), dbName, stbName);
            free(pids);
            free(groups);
            return TSDB_CODE_BACKUP_CREATE_THREAD_FAILED;
        }
    }

    // wait threads
    for (int i = 0; i < count; i++) {
        pthread_join(pids[i], NULL);
    }

    // free
    free(pids);
    free(groups);
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
int backupMain(){
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