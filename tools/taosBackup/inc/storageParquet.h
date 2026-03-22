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

#ifndef INC_STORAGE_PARQUET_H_
#define INC_STORAGE_PARQUET_H_

#ifndef _WIN32

#include "bck.h"

int resultToFileParquet(TAOS_RES *res, const char *fileName, int64_t *outRows);
int fileParquetToStmt(TAOS_STMT *stmt, const char *fileName, int64_t *outRows);
int fileParquetToStmt2(TAOS_STMT2 *stmt2, const char *fileName, int64_t *outRows);

#else  /* _WIN32 — Parquet not supported on Windows */

#include <taos.h>

static inline int resultToFileParquet(TAOS_RES *res, const char *fileName, long long *outRows) {
    (void)res; (void)fileName; (void)outRows; return -1;
}
static inline int fileParquetToStmt(TAOS_STMT *stmt, const char *fileName, long long *outRows) {
    (void)stmt; (void)fileName; (void)outRows; return -1;
}
static inline int fileParquetToStmt2(TAOS_STMT2 *stmt2, const char *fileName, long long *outRows) {
    (void)stmt2; (void)fileName; (void)outRows; return -1;
}

#endif  /* _WIN32 */

#endif  // INC_STORAGE_PARQUET_H_
