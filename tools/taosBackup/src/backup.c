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
    
#include "backup.h"
#include "backupMeta.h"
#include "backupData.h"

//
// -------------------------------------- UTIL -----------------------------------------
//

int genBackFileName(BackFileType fileType, const char *dbName, const char *tableName, char *fileName, int len) {
    return TSDB_CODE_SUCCESS;
}


//
// ------------------- main ---------------------
//

//
// backup database
//
int backDatabase(const char *dbName) {
    int code = TSDB_CODE_FAILED;
    DBInfo dbInfo;
    dbInfo.dbName = dbName;

    // meta
    code = backDatabaseMeta(&dbInfo);
    if (code != TSDB_CODE_SUCCESS) {
        printf("backup database meta failed, code: %d\n", code);
        return code;
    }

    // data
    code = backDatabaseData(&dbInfo);
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
    int code = TSDB_CODE_FAILED;

    // get backup databases
    char **backDB = argsGetBackDB();
    if (backDB == NULL) {
        printf("no database to backup\n");
        return TSDB_CODE_BACKUP_INVALID_PARAM;
    }

    // loop backup each database
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
