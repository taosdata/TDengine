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

#ifndef INC_BCKBACKUP_H_
#define INC_BCKBACKUP_H_

#include <taos.h>
#include <taoserror.h>

#include "bckArgs.h"

//
// ---------------- define ----------------
//


//
// ---------------- struct ----------------
//
typedef struct GroupThread {
    char ** childTableNames;
    int numChildTables;
    char dbName[TSDB_DB_NAME_LEN];
    char stbName[TSDB_TABLE_NAME_LEN];
    TAOS* conn;
} GroupThread;


//
// ---------------- util ----------------
//

typedef enum  {
    BACK_FILE_TYPE_META = 0,
    BACK_FILE_TYPE_DATA = 1,
} BackFileType;

int genBackFileName(BackFileType fileType, const char *dbName, const char *tableName, char *fileName, int len);

int genBackTableSql(const char *dbName, const char *tableName, char *sql, int len);


// backup main function
int backupMain();

int backDBMeta(const char *dbName);
int backDBData(const char *dbName);



#endif  // INC_BCKBACKUP_H_
