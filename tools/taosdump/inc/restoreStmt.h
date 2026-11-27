/*
 * Copyright (c) 2025 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#ifndef INC_RESTORESTMT_H_
#define INC_RESTORESTMT_H_

#include "bck.h"
#include "blockReader.h"
#include "colCompress.h"

// forward declaration
struct StbChange;

// STMT1 (legacy) restore context
typedef struct {
    TAOS_STMT  *stmt;
    TAOS       *conn;
    const char *dbName;
    char        tbName[TSDB_TABLE_NAME_LEN];
    int         numFields;
    FieldInfo  *fieldInfos;
    int64_t     totalRows;
    int64_t     batchRows;
    int64_t     pendingRows;
    bool        prepared;
    TAOS_MULTI_BIND *bindArray;
    int              bindArrayCap;
    struct StbChange *stbChange;
    // Last opened file's size in bytes (from taosFile->fileSize); used by
    // restoreDataThread to skip the per-file taosStatFile syscall.
    int64_t          lastFileSize;
} StmtRestoreCtx;

// Initialize STMT
TAOS_STMT* initStmt(TAOS* taos, bool single);

// Restore one data file using STMT v1
int restoreOneDataFile(StmtRestoreCtx *ctx, const char *filePath);

// Restore one parquet file using STMT v1
int restoreOneParquetFile(TAOS_STMT **stmtPtr, TAOS *conn, const char *dbName,
                          const char *filePath, struct StbChange *stbChange, int64_t *rowsOut);

// Free bind array
void freeBindArray(TAOS_MULTI_BIND *bindArray, int numFields);

// Reset STMT on error
bool stmtResetOnError(StmtRestoreCtx *ctx);

#endif  // INC_RESTORESTMT_H_
