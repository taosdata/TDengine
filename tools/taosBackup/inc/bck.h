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
#include "bckPool.h"

// ---------------- define ----------------
#define MAX_PATH_LEN 512
#define FOLDER_MAXFILE 100000


// ---------------- enum ----------------

typedef enum  {
    BACK_FILE_DBSQL   = 0,
    BACK_FILE_STBJSON = 1,
    BACK_FILE_TAG     = 2,
    BACK_FILE_DATA    = 3
} BackFileType;

typedef enum  {
    BINARY_TAOS = 0,
    BINARY_PARQUET = 1,
} StorageFormat;


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
