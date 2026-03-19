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

#ifndef INC_RESTORESTMT2_H_
#define INC_RESTORESTMT2_H_

#include "bck.h"
#include "blockReader.h"
#include "colCompress.h"

// forward declaration
struct StbChange;

// Maximum number of CTBs accumulated into a single TAOS_STMT2_BINDV call
#define STMT2_MULTI_TABLE_PENDING  64

// One pending slot: holds the accumulated colBinds for a single CTB file
typedef struct Stmt2TableSlot {
    char              fqn[TSDB_DB_NAME_LEN + TSDB_TABLE_NAME_LEN + 8]; // `db`.`tbl`
    TAOS_STMT2_BIND  *colBinds;       // per-column arrays (owns all buffer/length/is_null)
    int               colBindsCap;    // allocated column count in colBinds
    int               numCols;        // rows' column binding count (<= colBindsCap)
    int32_t           numRows;        // accumulated rows in this slot
    int32_t          *varWriteOffsets;// packed byte-write offsets per var-type column
    int32_t          *varBufCapacity; // byte capacity per var-type column
} Stmt2TableSlot;

// STMT2 (v3.3+) restore context
typedef struct Stmt2RestoreCtx {
    TAOS_STMT2  *stmt2;
    TAOS        *conn;
    const char  *dbName;
    char         tbName[TSDB_TABLE_NAME_LEN];
    int          numFields;
    FieldInfo   *fieldInfos;
    int64_t      totalRows;
    int64_t      accRows;
    TAOS_STMT2_BIND *colBinds;
    int              colBindsCap;
    int64_t          rowBufCap;
    int32_t         *varWriteOffsets;
    int32_t         *varBufCapacity;
    struct StbChange *stbChange;
    bool         prepared;
    bool             multiTable;
    Stmt2TableSlot  *pendingSlots;
    int              numPending;
    int64_t          totalPendingRows;
} Stmt2RestoreCtx;

// Initialize STMT2
TAOS_STMT2* initStmt2(TAOS* taos, bool single);

// Restore one data file using STMT2 v2
int restoreOneDataFileV2(Stmt2RestoreCtx *ctx, const char *filePath);

// Restore one parquet file using STMT2 v2
int restoreOneParquetFileV2(TAOS_STMT2 **stmt2Ptr, TAOS *conn, const char *dbName,
                            const char *filePath, struct StbChange *stbChange, int64_t *rowsOut);

// Free a table slot
void stmt2FreeSlot(Stmt2TableSlot *slot);

// Reset STMT2 on error
bool stmt2ResetOnError(Stmt2RestoreCtx *ctx);

// Free column buffers
void stmt2FreeColBuffers(Stmt2RestoreCtx *ctx);

// Flush multi-table slots
int stmt2FlushMultiTableSlots(Stmt2RestoreCtx *ctx);

#endif  // INC_RESTORESTMT2_H_
