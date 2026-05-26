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

#ifndef INC_BCKDB_H_
#define INC_BCKDB_H_
#include "bck.h"
#include <taos.h>
#include <taoserror.h>


// ---------------- define ----------------


// ---------------- interface ----------------
char ** getDBSuperTableNames(const char *dbName, int *code);
char ** getDBNormalTableNames(const char *dbName, int *code);
char ** getDBVirtualTableNames(const char *dbName, int *code);
int     getDBNormalTableCount(const char *dbName, int32_t *outCount);

int queryValueInt(const char *sql, int col, int32_t *outValue);

#endif  // INC_BCKDB_H_
