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

#ifndef INC_RESTOREMETA_H_
#define INC_RESTOREMETA_H_

#include "bck.h"
#include "restore.h"

//
// ---------------- struct ----------------
//

// restore tag thread
typedef struct {
    DBInfo*   dbInfo;
    StbInfo*  stbInfo;
    int       index;       // thread index (1-based)
    TAOS*     conn;
    TdThread  pid;
    char      tagFile[MAX_PATH_LEN];  // tag data file to restore
    int32_t   code;    // first error code from this thread
} RestoreTagThread;


//
// ---------------- interface ----------------
//

// restore database meta: create db, create stb, create child tables with tags
int restoreDatabaseMeta(const char *dbName);

// restore database extended meta: virtual tables, streams, topics.
// Must be called only after every database's physical tables exist, because a
// virtual table may reference tables in other databases.
// needCreateDb: true when the basic stage did not run for this database
//               (--content=ext-meta), so the database must be created first.
int restoreDatabaseExtMeta(const char *dbName, bool needCreateDb);

// Validate that every user-specified table name (positional args) exists in
// the backup files for the given database.  Must be called BEFORE any restore
// work begins.  Returns TSDB_CODE_SUCCESS if all tables are found, or
// TSDB_CODE_BCK_SPEC_TABLE_NOT_FOUND listing every missing name.
int validateSpecTablesForRestore(const char *dbName);

#endif  // INC_RESTOREMETA_H_
