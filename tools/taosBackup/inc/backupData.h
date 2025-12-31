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

#ifndef INC_BACKUPDATA_H_
#define INC_BACKUPDATA_H_

#include "backup.h"

//
// ---------------- define ----------------
//


//
// ---------------- struct ----------------
//

// data
typedef struct DataThread {
    char ** childTableNames;
    int numChildTables;
    char dbName[TSDB_DB_NAME_LEN];
    char stbName[TSDB_TABLE_NAME_LEN];
    TAOS* conn;
    pthread_t pid;
} DataThread;


//
// ---------------- interface ----------------
//


#endif  // INC_BACKUPDATA_H_
