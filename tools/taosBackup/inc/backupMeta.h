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
    int       index;
    TAOS*     conn;
    TdThread  pid;
    int32_t   code;    // first error code from this thread
} TagThread;



//
// ---------------- interfalce ----------------
//
int  backDatabaseMeta(DBInfo *dbInfo);
bool isVirtualSuperTable(const char *dbName, const char *stbName);

//
// ---------------- function ----------------
//

#endif  // INC_BACKUPMETA_H_
