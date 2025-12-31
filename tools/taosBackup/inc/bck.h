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

#ifndef INC_BCK_H_
#define INC_BCK_H_
// sys
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <errno.h>
// taos
#include <taos.h>
#include <tdef.h>
#include <taoserror.h>
// bck
#include "bck.h"
#include "bckUtil.h"
#include "bckArgs.h"
#include "bckLog.h"
#include "bckError.h"
#include "bckFile.h"
#include "bckDb.h"

// ---------------- define ----------------
#define MAX_PATH_LEN 512
#define FILE_DBSQL   "db.sql"
#define FILE_DBJSON  "db.json"

// ---------------- enum ----------------

typedef enum  {
    BACK_FILE_TYPE_META = 0,
    BACK_FILE_TYPE_DATA = 1,
} BackFileType;

// ---------------- struct ----------------

// db
typedef struct {
    const char *dbName;
} DBInfo;

// stb
typedef struct {
    DBInfo     *dbInfo;
    const char *stbName;
    const char *selectTags;
} StbInfo;

#endif  // INC_BCK_H_
