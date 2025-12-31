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

#ifndef INC_BACKUPMETA_H_
#define INC_BACKUPMETA_H_

#include "backup.h"

//
// ---------------- define ----------------
//


//
// ---------------- struct ----------------
//

// tag
typedef struct TagThread {
    DBInfo*   dbInfo;
    StbInfo*  stbInfo;
    int       limit;
    int       offset;
    TAOS*     conn;
    pthread_t pid;
} TagThread;



//
// ---------------- interfalce ----------------
//
int backDBMeta(const char *dbName);

//
// ---------------- function ----------------
//
int genBackTableSql(const char *dbName, const char *tableName, char *sql, int len);


#endif  // INC_BACKUPMETA_H_
