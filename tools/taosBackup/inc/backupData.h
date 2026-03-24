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
typedef struct {
    DBInfo*   dbInfo;
    StbInfo*  stbInfo;
    int       limit;
    int       offset;
    int       index;
    TAOS*     conn;
    pthread_t pid;
    int32_t   code;    // first error code from this thread
    // Thread-level reusable buffers (space-for-time optimization)
    char*     writeBuf;     // 4MB write buffer, reused across all CTBs
    int32_t   writeBufCap;
    char      stbDirPath[4096];  // pre-built directory path for this stb
    bool      stbDirCreated;
} DataThread;


//
// ---------------- interface ----------------
//
int backDatabaseData(DBInfo *dbInfo);

#endif  // INC_BACKUPDATA_H_
