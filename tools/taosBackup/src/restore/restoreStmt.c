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

#include "restoreStmt.h"
#include "restoreData.h"
#include "storageTaos.h"
#include "storageParquet.h"
#include "parquetBlock.h"
#include "colCompress.h"
#include "blockReader.h"
#include "bckPool.h"
#include "bckDb.h"
#include "bckSchemaChange.h"
#include "decimal.h"
#include "ttypes.h"
#include "osString.h"
#include "bckProgress.h"

// init stmt
TAOS_STMT* initStmt(TAOS* taos, bool single) {
    if (!single) {
        return taos_stmt_init(taos);
    }

    TAOS_STMT_OPTIONS op;
    memset(&op, 0, sizeof(op));
    op.singleStbInsert      = single;
    op.singleTableBindOnce  = single;
    return taos_stmt_init_with_options(taos, &op);
}

//
// Prepare STMT for the first file in a thread:
//   INSERT INTO ? VALUES(?,?,?...)
// The `?` placeholder for the table name is a TDengine extension that enables
// efficient child-table switching via taos_stmt_set_tbname() without a
// full re-prepare round-trip.
//
static int stmtPrepareInsert(StmtRestoreCtx *ctx) {
    char *stmtBuf = (char *)taosMemoryMalloc(TSDB_MAX_SQL_LEN);
    if (stmtBuf == NULL) {
        logError("malloc stmt buffer failed");
        return TSDB_CODE_BCK_MALLOC_FAILED;
    }

    // Determine number of bind columns (may be fewer with schema change)
    const char *partCols = "";
    int bindColCount = ctx->numFields;
    if (ctx->stbChange && ctx->stbChange->schemaChanged && ctx->stbChange->partColsStr) {
        partCols = ctx->stbChange->partColsStr;
        bindColCount = ctx->stbChange->matchColCount;
        logInfo("partial column write for %s.%s: %s (bind %d of %d cols)",
                ctx->dbName, ctx->tbName, partCols, bindColCount, ctx->numFields);
    }

    // Always use ? for the table name so that taos_stmt_set_tbname() can
    // switch child tables without a new prepare round-trip.
    char *p = stmtBuf;
    p += snprintf(p, TSDB_MAX_SQL_LEN, "INSERT INTO ? %s VALUES(?", partCols);
    for (int i = 1; i < bindColCount; i++) {
        p += sprintf(p, ",?");
    }
    p += sprintf(p, ")");

    logDebug("stmt prepare: %s", stmtBuf);

    int ret = taos_stmt_prepare(ctx->stmt, stmtBuf, 0);
    if (ret != 0) {
        logError("taos_stmt_prepare failed: %s, sql: %s", taos_stmt_errstr(ctx->stmt), stmtBuf);
        taosMemoryFree(stmtBuf);
        return ret;
    }
    taosMemoryFree(stmtBuf);

    // Immediately bind the first child table name.
    // Use plain backtick-quoted table name (no db prefix) because taos_select_db
    // has already set the default database on the connection — matching benchInsert.c.
    char fqn[TSDB_TABLE_NAME_LEN + 4];
    snprintf(fqn, sizeof(fqn), "`%s`", ctx->tbName);
    ret = taos_stmt_set_tbname(ctx->stmt, fqn);
    if (ret != 0) {
        logError("taos_stmt_set_tbname (initial) failed (%s): %s", fqn, taos_stmt_errstr(ctx->stmt));
        return ret;
    }

    ctx->prepared = true;
    return TSDB_CODE_SUCCESS;
}

//
// Switch an already-prepared STMT to a different child table.
// Uses taos_stmt_set_tbname() which avoids a full re-prepare round-trip.
// The STMT must already have been prepared for a table with the same
// column count (i.e. same super table).
//
static int stmtSwitchTable(StmtRestoreCtx *ctx, const char *tbName) {
    /* plain backtick-quoted table name — taos_select_db already set default db */
    char fqn[TSDB_TABLE_NAME_LEN + 4];
    snprintf(fqn, sizeof(fqn), "`%s`", tbName);
    int ret = taos_stmt_set_tbname(ctx->stmt, fqn);
    if (ret != 0) {
        logError("taos_stmt_set_tbname failed (%s): %s", fqn, taos_stmt_errstr(ctx->stmt));
        return ret;
    }
    strncpy(ctx->tbName, tbName, TSDB_TABLE_NAME_LEN - 1);
    ctx->tbName[TSDB_TABLE_NAME_LEN - 1] = '\0';
    return TSDB_CODE_SUCCESS;
}

//
// Reset STMT after an error.
// Abandons the broken stmt handle (without closing it — closing would cause a
// heap-use-after-free when taos_stmt_execute already freed the vgroup submit
// data internally via qBuildStmtFinOutput → tDestroySubmitTbData), allocates
// a fresh one, and marks context as not-prepared so the next file will call
// stmtPrepareInsert() again.
// Returns true on success, false if taos_stmt_init also fails.
//
bool stmtResetOnError(StmtRestoreCtx *ctx) {
    if (ctx->stmt) {
        // Do NOT call taos_stmt_close here.  If taos_stmt_execute was already
        // called on this handle, the internal vgroup submit data has been freed
        // by the execute path (qBuildStmtFinOutput → tDestroySubmitTbData).
        // Calling taos_stmt_close → stmtCleanSQLInfo → qDestroyQuery would try
        // to destroy that already-freed memory, triggering a heap-use-after-free.
        // Instead, simply abandon the handle; the process is short-lived so the
        // leak is acceptable.
        ctx->stmt = NULL;
    }
    ctx->prepared    = false;
    ctx->batchRows   = 0;
    ctx->pendingRows = 0;
    TAOS_STMT *newStmt = initStmt(ctx->conn, true);
    if (newStmt == NULL) {
        logError("stmtResetOnError: initStmt failed — connection may be broken");
        return false;
    }
    ctx->stmt = newStmt;
    return true;
}

//
// Free bind array buffers
//
void freeBindArray(TAOS_MULTI_BIND *bindArray, int numFields) {
    for (int i = 0; i < numFields; i++) {
        TAOS_MULTI_BIND *bind = &bindArray[i];
        if (bind->buffer) {
            taosMemoryFree(bind->buffer);
            bind->buffer = NULL;
        }
        if (bind->length) {
            taosMemoryFree(bind->length);
            bind->length = NULL;
        }
        if (bind->is_null) {
            taosMemoryFree(bind->is_null);
            bind->is_null = NULL;
        }
    }
}

static int bindBlockData(StmtRestoreCtx *ctx,
                         void *blockData,
                         int32_t blockRows,
                         FieldInfo *fieldInfos,
                         int numFields,
                         TAOS_MULTI_BIND *bindArray) {
    // Parse block
    BlockReader reader;
    int32_t code = initBlockReader(&reader, blockData);
    if (code != TSDB_CODE_SUCCESS) {
        logError("init block reader failed: %d", code);
        return code;
    }

    // Process each column, skipping columns not in partial write set
    for (int c = 0; c < numFields; c++) {
        // Determine bind index (with schema change, some backup cols are skipped)
        int bindIdx = getPartialWriteBindIdx(ctx->stbChange, c);
        if (bindIdx < 0) {
            // This column is not in the server schema, skip but still advance reader
            void *skipData = NULL;
            int32_t skipLen = 0;
            code = getColumnData(&reader, fieldInfos[c].type, &skipData, &skipLen);
            if (code != TSDB_CODE_SUCCESS) {
                logError("skip column data failed: col=%d", c);
                return code;
            }
            continue;
        }
        void *colData = NULL;
        int32_t colDataLen = 0;
        code = getColumnData(&reader, fieldInfos[c].type, &colData, &colDataLen);
        if (code != TSDB_CODE_SUCCESS) {
            logError("get column data failed: col=%d", c);
            return code;
        }

        TAOS_MULTI_BIND *bind = &bindArray[bindIdx];
        /* Save previous block's buffer pointers and row-count capacity BEFORE
         * the memset that resets the bind struct for the current block. */
        void    *prevBuffer  = bind->buffer;
        void    *prevLength  = bind->length;
        void    *prevIsNull  = bind->is_null;
        int32_t  prevNumRows = (int32_t)bind->num;  /* capacity tracker */
        memset(bind, 0, sizeof(TAOS_MULTI_BIND));
        bind->buffer_type = fieldInfos[c].type;
        bind->num = blockRows;

        if (IS_VAR_DATA_TYPE(fieldInfos[c].type)) {
            // Variable type layout: offsets[blockRows] (int32_t each) + raw data
            int32_t *offsets = (int32_t *)colData;
            char *varDataBase = (char *)colData + blockRows * sizeof(int32_t);
            bool isNchar = (fieldInfos[c].type == TSDB_DATA_TYPE_NCHAR);

            // Space-for-time: use declared field width as buffer stride.
            // This eliminates the two-pass maxLen scan and lets us reuse
            // buffer/length/is_null across blocks (bind->num is the capacity).
            int32_t stride = fieldInfos[c].bytes;  // declared max bytes per row

            // Reuse or grow per-column buffers based on capacity (prevNumRows).
            char    *buffer  = (char    *)prevBuffer;
            int32_t *lengths = (int32_t *)prevLength;
            char    *isNull  = (char    *)prevIsNull;
            if (prevNumRows < blockRows) {
                char    *nb = (char    *)taosMemoryRealloc(buffer,  (size_t)blockRows * stride);
                if (!nb) { bind->buffer = buffer; return TSDB_CODE_BCK_MALLOC_FAILED; }
                bind->buffer = nb; buffer = nb;

                int32_t *nl = (int32_t *)taosMemoryRealloc(lengths, (size_t)blockRows * sizeof(int32_t));
                if (!nl) { bind->length = lengths; return TSDB_CODE_BCK_MALLOC_FAILED; }
                bind->length = nl; lengths = nl;

                char    *nn = (char    *)taosMemoryRealloc(isNull,  (size_t)blockRows);
                if (!nn) { bind->is_null = isNull; return TSDB_CODE_BCK_MALLOC_FAILED; }
                bind->is_null = nn; isNull = nn;
            }

            // Zero out the region we'll use (stride-packed layout).
            memset(buffer, 0, (size_t)blockRows * stride);
            memset(isNull,  0, blockRows);
            memset(lengths, 0, (size_t)blockRows * sizeof(int32_t));

            // For NCHAR: raw block stores UCS-4 (4 bytes/char), STMT expects UTF-8.
            // We use a temporary conversion buffer (local, freed after each block).
            char    *convBuf  = NULL;
            int32_t *utf8Lens = NULL;
            if (isNchar) {
                convBuf  = (char    *)taosMemoryMalloc((size_t)stride * blockRows);
                utf8Lens = (int32_t *)taosMemoryMalloc((size_t)blockRows * sizeof(int32_t));
                if (!convBuf || !utf8Lens) {
                    taosMemoryFree(convBuf);
                    taosMemoryFree(utf8Lens);
                    return TSDB_CODE_BCK_MALLOC_FAILED;
                }
                memset(utf8Lens, 0, (size_t)blockRows * sizeof(int32_t));
            }

            int32_t convOffset = 0;
            for (int row = 0; row < blockRows; row++) {
                if (offsets[row] < 0) {
                    isNull[row]  = 1;
                    lengths[row] = 0;
                } else {
                    isNull[row] = 0;
                    uint16_t varLen = *(uint16_t *)(varDataBase + offsets[row]);
                    if (isNchar && varLen > 0) {
                        char *ucs4Data = varDataBase + offsets[row] + sizeof(uint16_t);
                        char *utf8Out  = convBuf + convOffset;
                        int32_t utf8Len = taosUcs4ToMbs((TdUcs4 *)ucs4Data, varLen, utf8Out, NULL);
                        if (utf8Len < 0) {
                            logError("UCS-4 to UTF-8 conversion failed: col=%d row=%d", c, row);
                            utf8Len = 0;
                        }
                        utf8Lens[row] = utf8Len;
                        lengths[row]  = utf8Len;
                        convOffset   += utf8Len;
                    } else {
                        lengths[row] = varLen;
                    }
                }
            }

            // Write data into the stride buffer.
            if (isNchar) {
                int32_t srcOff = 0;
                for (int row = 0; row < blockRows; row++) {
                    if (!isNull[row] && utf8Lens[row] > 0) {
                        memcpy(buffer + row * stride, convBuf + srcOff, utf8Lens[row]);
                        srcOff += utf8Lens[row];
                    }
                }
                taosMemoryFree(convBuf);
                taosMemoryFree(utf8Lens);
            } else {
                for (int row = 0; row < blockRows; row++) {
                    if (!isNull[row] && lengths[row] > 0) {
                        char *src = varDataBase + offsets[row] + sizeof(uint16_t);
                        memcpy(buffer + row * stride, src, lengths[row]);
                    }
                }
            }

            bind->buffer        = buffer;
            bind->buffer_length = stride;
            bind->length        = lengths;
            bind->is_null       = isNull;

        } else if (IS_DECIMAL_TYPE(fieldInfos[c].type)) {
            /* DECIMAL: raw block has bitmap[(blockRows+7)/8] + fixed bytes
             * (8 for DECIMAL, 16 for DECIMAL128) per row.
             * STMT1 requires buffer_type == column type and raw binary data. */
            int32_t actualBytes = fieldGetRawBytes(&fieldInfos[c]);
            int32_t bitmapLen   = (blockRows + 7) / 8;
            char   *bitmap      = (char *)colData;
            char   *fixData     = (char *)colData + bitmapLen;

            // Space-for-time: reuse buffer/lengths/is_null via prevNumRows capacity.
            char    *buffer  = (char    *)prevBuffer;
            int32_t *lengths = (int32_t *)prevLength;
            char    *isNull  = (char    *)prevIsNull;
            if (prevNumRows < blockRows) {
                char    *nb = (char    *)taosMemoryRealloc(buffer,  (size_t)blockRows * actualBytes);
                if (!nb) { 
                    bind->buffer = buffer; 
                    return TSDB_CODE_BCK_MALLOC_FAILED; 
                }
                bind->buffer = nb; buffer = nb;

                int32_t *nl = (int32_t *)taosMemoryRealloc(lengths, (size_t)blockRows * sizeof(int32_t));
                if (!nl) { 
                    bind->length = lengths; 
                    return TSDB_CODE_BCK_MALLOC_FAILED; 
                }
                bind->length = nl; lengths = nl;

                char    *nn = (char    *)taosMemoryRealloc(isNull,  (size_t)blockRows);
                if (!nn) { 
                    bind->is_null = isNull; 
                    return TSDB_CODE_BCK_MALLOC_FAILED; 
                }
                bind->is_null = nn; isNull = nn;
            }

            memset(isNull,  0, blockRows);
            memset(lengths, 0, (size_t)blockRows * sizeof(int32_t));
            for (int row = 0; row < blockRows; row++) {
                if (bitmap[row >> 3] & (1u << (7 - (row & 7)))) {
                    isNull[row]  = 1;
                    lengths[row] = 0;
                } else {
                    isNull[row]  = 0;
                    lengths[row] = actualBytes;
                    memcpy(buffer + row * actualBytes,
                           fixData + row * actualBytes,
                           actualBytes);
                }
            }

            /* buffer_type was already set to fieldInfos[c].type (DECIMAL/DECIMAL128)
             * above; keep it — pass raw binary to STMT1. */
            bind->buffer        = buffer;
            bind->buffer_length = actualBytes;
            bind->length        = lengths;
            bind->is_null       = isNull;
        } else {
            // Fixed type layout: bitmap[(blockRows+7)/8] + data[blockRows * bytes]
            int32_t bitmapLen = (blockRows + 7) / 8;
            char *bitmap = (char *)colData;
            char *fixData = (char *)colData + bitmapLen;
            int32_t typeBytes = fieldInfos[c].bytes;

            // Space-for-time: reuse buffer/is_null when prevNumRows >= blockRows.
            char *buffer = (char *)prevBuffer;
            char *isNull  = (char *)prevIsNull;
            if (prevNumRows < blockRows) {
                char *nb = (char *)taosMemoryRealloc(buffer, (size_t)blockRows * typeBytes);
                if (!nb) { 
                    bind->buffer = buffer; 
                    return TSDB_CODE_BCK_MALLOC_FAILED; 
                }
                bind->buffer = nb; buffer = nb;

                char *nn = (char *)taosMemoryRealloc(isNull, (size_t)blockRows);
                if (!nn) { 
                    bind->is_null = isNull; 
                    return TSDB_CODE_BCK_MALLOC_FAILED; 
                }
                bind->is_null = nn; isNull = nn;
            }

            // Copy all data in one shot; then build is_null with space-for-time
            // sparse bitmap scan: assume non-null (common), skip zero bitmap bytes.
            memcpy(buffer, fixData, blockRows * typeBytes);
            memset(isNull, 0, blockRows);
            for (int bi = 0; bi < bitmapLen; bi++) {
                uint8_t byt = (uint8_t)bitmap[bi];
                if (!byt) continue;  /* fast path: no nulls in this 8-row group */
                int base8 = bi * 8;
                for (int bit = 7; bit >= 0; bit--) {
                    int row = base8 + (7 - bit);  /* TDengine: bit7=row0 (MSB-first) */
                    if (row >= blockRows) break;
                    if (byt & (1u << bit)) isNull[row] = 1;
                }
            }

            bind->buffer        = buffer;
            bind->buffer_length = typeBytes;
            bind->length        = NULL;  // fixed-length types don't need length array
            bind->is_null       = isNull;
        }
    }

    return TSDB_CODE_SUCCESS;
}

// Callback: process each decompressed data block with STMT batch insert
//
static int dataBlockCallback(void *userData,
                             FieldInfo *fieldInfos,
                             int numFields,
                             void *blockData,
                             int32_t blockLen,
                             int32_t blockRows) {
    StmtRestoreCtx *ctx = (StmtRestoreCtx *)userData;
    int code = TSDB_CODE_SUCCESS;
    (void)blockLen;

    if (g_interrupted) {
        return TSDB_CODE_BCK_USER_CANCEL;
    }

    if (blockRows == 0) {
        return TSDB_CODE_SUCCESS;
    }

    // Prepare STMT if not yet done
    if (!ctx->prepared) {
        code = stmtPrepareInsert(ctx);
        if (code != TSDB_CODE_SUCCESS) {
            return code;
        }
    }

    // Determine actual bind column count (may differ from numFields with schema change)
    int bindColCount = numFields;
    if (ctx->stbChange && ctx->stbChange->schemaChanged) {
        bindColCount = ctx->stbChange->matchColCount;
    }

    // Ensure pre-allocated bind array is large enough
    if (ctx->bindArray == NULL || ctx->bindArrayCap < bindColCount) {
        if (ctx->bindArray) {
            freeBindArray(ctx->bindArray, ctx->bindArrayCap);
            taosMemoryFree(ctx->bindArray);
        }
        ctx->bindArray = (TAOS_MULTI_BIND *)taosMemoryCalloc(bindColCount, sizeof(TAOS_MULTI_BIND));
        ctx->bindArrayCap = bindColCount;
        if (ctx->bindArray == NULL) {
            return TSDB_CODE_BCK_MALLOC_FAILED;
        }
    } else {
        /* Space-for-time: keep per-column buffers alive across blocks.
         * bindBlockData reuses buffer/is_null/length when bind->num (previous row
         * count == capacity) >= current blockRows.  Only reset non-capacity fields;
         * buffer, length, is_null, and num (capacity tracker) are preserved. */
        for (int _i = 0; _i < bindColCount; _i++) {
            ctx->bindArray[_i].buffer_type = 0;
        }
    }

    // Build bind array from block data (column-batch mode)
    code = bindBlockData(ctx, blockData, blockRows, fieldInfos, numFields, ctx->bindArray);
    if (code != TSDB_CODE_SUCCESS) {
        logError("bind block data failed: %d", code);
        return code;
    }

    // Bind parameters
    code = taos_stmt_bind_param_batch(ctx->stmt, ctx->bindArray);
    if (code != 0) {
        logError("taos_stmt_bind_param_batch failed: %s, table: %s",
                 taos_stmt_errstr(ctx->stmt), ctx->tbName);
        return code;
    }

    // Add batch
    code = taos_stmt_add_batch(ctx->stmt);
    if (code != 0) {
        logError("taos_stmt_add_batch failed: %s", taos_stmt_errstr(ctx->stmt));
        return code;
    }

    ctx->totalRows += blockRows;
    ctx->batchRows += blockRows;
    ctx->pendingRows += blockRows;   /* cross-file/cross-table row accumulator */

    // Execute when cross-file accumulated rows reach the batch threshold.
    // Using pendingRows (not batchRows) ensures sparse CTBs with 1-2 rows each
    // are batched across many files before an execute RPC is issued.
    int64_t batchThreshold = argDataBatch();
    if (ctx->pendingRows >= batchThreshold) {
        code = taos_stmt_execute(ctx->stmt);
        if (code != 0) {
            logError("taos_stmt_execute failed: %s, table: %s.%s, batchRows: %" PRId64,
                     taos_stmt_errstr(ctx->stmt), ctx->dbName, ctx->tbName, ctx->batchRows);
            return code;
        }
        ctx->batchRows   = 0;
        ctx->pendingRows = 0;  /* reset cross-file counter after execute */
    }

    return TSDB_CODE_SUCCESS;
}


int restoreOneParquetFile(TAOS_STMT **stmtPtr, TAOS *conn, const char *dbName,
                                  const char *filePath,
                                  StbChange *stbChange,
                                  int64_t   *rowsOut) {
    (void)stbChange;  /* reserved for future schema-change support */
    int code = TSDB_CODE_SUCCESS;

    // Extract table name:  .../{tbName}.par
    const char *baseName = strrchr(filePath, '/');
    if (baseName) baseName++; else baseName = filePath;

    char tbName[TSDB_TABLE_NAME_LEN] = {0};
    const char *dot = strrchr(baseName, '.');
    if (dot && (dot - baseName) < TSDB_TABLE_NAME_LEN) {
        memcpy(tbName, baseName, dot - baseName);
        tbName[dot - baseName] = '\0';
    } else {
        snprintf(tbName, sizeof(tbName), "%s", baseName);
    }

    logDebug("restore parquet file: %s -> table: %s.%s", filePath, dbName, tbName);

    // Open reader to discover schema (stored in file metadata)
    ParquetReader *pr = parquetReaderOpen(filePath, &code);
    if (pr == NULL) {
        logError("open parquet file failed(%d): %s", code, filePath);
        return code;
    }

    TAOS_FIELD *fields    = NULL;
    int         numFields = parquetReaderGetFields(pr, &fields);
    (void)fields;  /* we only need the count; actual field data stays in the reader */
    if (numFields <= 0) {
        logError("parquet file has no fields: %s", filePath);
        parquetReaderClose(pr);
        return TSDB_CODE_BCK_NO_FIELDS;
    }

    // Init STMT once per thread; re-prepare for each new table (avoids repeated init+close).
    if (!*stmtPtr) {
        *stmtPtr = initStmt(conn, true);
        if (!*stmtPtr) {
            logError("initStmt failed for %s.%s", dbName, tbName);
            parquetReaderClose(pr);
            return TSDB_CODE_BCK_STMT_FAILED;
        }
    }
    TAOS_STMT *stmt = *stmtPtr;

    char *stmtBuf = (char *)taosMemoryMalloc(TSDB_MAX_SQL_LEN);
    if (stmtBuf == NULL) {
        parquetReaderClose(pr);
        return TSDB_CODE_BCK_MALLOC_FAILED;
    }
    char *p = stmtBuf;
    p += snprintf(p, TSDB_MAX_SQL_LEN,
                  "INSERT INTO `%s`.`%s` VALUES(?",
                  argRenameDb(dbName), tbName);
    for (int i = 1; i < numFields; i++)
        p += sprintf(p, ",?");
    sprintf(p, ")");

    logDebug("parquet stmt prepare: %s", stmtBuf);
    int ret = taos_stmt_prepare(stmt, stmtBuf, 0);
    taosMemoryFree(stmtBuf);
    if (ret != 0) {
        logError("taos_stmt_prepare failed: %s", taos_stmt_errstr(stmt));
        // Invalidate handle so next file gets a fresh one
        taos_stmt_close(stmt);
        *stmtPtr = NULL;
        parquetReaderClose(pr);
        return ret;
    }

    // Stream all row-groups through the STMT via fileParquetToStmt
    // (closes the internal reader; the schema was already confirmed above)
    parquetReaderClose(pr);

    int64_t rows = 0;
    code = fileParquetToStmt(stmt, filePath, &rows);
    if (code != TSDB_CODE_SUCCESS) {
        logError("restore parquet failed(%d): %s -> %s.%s",
                 code, filePath, dbName, tbName);
    } else {
        logDebug("restore parquet done: %s.%s rows: %" PRId64, dbName, tbName, rows);
        if (rowsOut) *rowsOut = rows;
    }

    // stmt is NOT closed here; the caller owns it and closes after all files are done
    return code;
}

int restoreOneDataFile(StmtRestoreCtx *ctx,
                               const char     *filePath) {
    int code = TSDB_CODE_SUCCESS;

    // Extract table name from file path: .../{tbName}.dat
    const char *baseName = strrchr(filePath, '/');
    if (baseName) baseName++; else baseName = filePath;

    char tbName[TSDB_TABLE_NAME_LEN] = {0};
    const char *dot = strrchr(baseName, '.');
    if (dot && (dot - baseName) < TSDB_TABLE_NAME_LEN) {
        memcpy(tbName, baseName, dot - baseName);
        tbName[dot - baseName] = '\0';
    } else {
        snprintf(tbName, sizeof(tbName), "%s", baseName);
    }

    logDebug("restore data file: %s -> table: %s.%s", filePath, ctx->dbName, tbName);

    // Open .dat file
    TaosFile *taosFile = openTaosFileForRead(filePath, &code);
    if (taosFile == NULL) {
        logError("open data file failed(%d): %s", code, filePath);
        return code;
    }

    if (taosFile->header.numRows == 0) {
        logDebug("data file has no rows: %s", filePath);
        closeTaosFileRead(taosFile);
        return TSDB_CODE_SUCCESS;
    }

    ctx->lastFileSize = taosFile->fileSize;  /* cache for caller's stat elimination */

    ctx->numFields  = taosFile->header.numFields;
    ctx->fieldInfos = (FieldInfo *)taosFile->header.schema;
    /* Do NOT reset ctx->totalRows here: it is a cumulative counter used
     * by the thread-loop stats (fileRows = bCtx.totalRows - rowsBefore).  */
    ctx->batchRows  = 0;
    int64_t fileRowsStart = ctx->totalRows;  /* for per-file log below */

    if (!ctx->prepared) {
        // First file: full prepare with concrete table name
        strncpy(ctx->tbName, tbName, TSDB_TABLE_NAME_LEN - 1);
        ctx->tbName[TSDB_TABLE_NAME_LEN - 1] = '\0';
        code = stmtPrepareInsert(ctx);
        if (code != TSDB_CODE_SUCCESS) {
            closeTaosFileRead(taosFile);
            return code;
        }
    } else {
        // Subsequent files: switch target table without re-preparing
        code = stmtSwitchTable(ctx, tbName);
        if (code != TSDB_CODE_SUCCESS) {
            closeTaosFileRead(taosFile);
            return code;
        }
    }

    // Read blocks and write via STMT
    code = readTaosFileBlocks(taosFile, dataBlockCallback, ctx);
    if (code != TSDB_CODE_SUCCESS) {
        logError("restore data blocks failed(%d): %s -> %s.%s",
                 code, filePath, ctx->dbName, tbName);
    } else {
        // Rows are flushed by the cross-file pendingRows threshold in dataBlockCallback.
        // The final flush for all remaining rows is done in restoreDataThread after the
        // file loop, so we intentionally skip per-file taos_stmt_execute here.
        logDebug("restore data file done: %s.%s rows: %" PRId64,
                 ctx->dbName, tbName, ctx->totalRows - fileRowsStart);
    }

    closeTaosFileRead(taosFile);
    return code;
}
