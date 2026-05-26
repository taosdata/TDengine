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

#include "restoreStmt2.h"
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

// Maximum rows per STMT2 bind_param call — equals STMT2_BATCH_MAX from bckArgs.h.
#define STMT2_MAX_ROWS_PER_BATCH  STMT2_BATCH_MAX

// Minimum row-buffer capacity: must hold at least one complete raw TDengine block.
#define STMT2_MIN_ROW_BUF_CAP  4096

TAOS_STMT2* initStmt2(TAOS* taos, bool single) {
    TAOS_STMT2_OPTION op2;
    memset(&op2, 0, sizeof(op2));
    op2.singleStbInsert      = single;
    op2.singleTableBindOnce  = single;

    TAOS_STMT2* stmt2 = taos_stmt2_init(taos, &op2);
    if (stmt2)
        logDebug("succ  taos_stmt2_init single=%d\n", single);
    else
        logError("failed taos_stmt2_init single=%d\n", single);
    return stmt2;
}

//
// Free all per-column buffers inside ctx->colBinds.
// The colBinds array itself is freed separately.
//
void stmt2FreeColBuffers(Stmt2RestoreCtx *ctx) {
    if (!ctx->colBinds) return;
    for (int i = 0; i < ctx->colBindsCap; i++) {
        TAOS_STMT2_BIND *b = &ctx->colBinds[i];
        taosMemoryFree(b->buffer);  b->buffer  = NULL;
        taosMemoryFree(b->length);  b->length  = NULL;
        taosMemoryFree(b->is_null); b->is_null = NULL;
    }
    taosMemoryFree(ctx->varWriteOffsets); ctx->varWriteOffsets = NULL;
    taosMemoryFree(ctx->varBufCapacity);  ctx->varBufCapacity  = NULL;
}

//
// Allocate (or reuse) per-column accumulation buffers able to hold
// at least `numRows` rows for `numCols` columns described by `fieldInfos`.
// Reuses existing buffers if capacity is sufficient.
//
static int stmt2AllocColBuffers(Stmt2RestoreCtx *ctx, int64_t numRows,
                                int numCols, FieldInfo *fieldInfos) {
    /* Determine effective batch cap: user override via --data-batch, else STMT2 default */
    int64_t batchCap = (argDataBatch() > 0) ? (int64_t)argDataBatch() : (int64_t)STMT2_BATCH_DEFAULT;
    /* clamp requested row count so we never exceed the effective batch cap */
    int64_t targetCap = (numRows < batchCap) ? numRows : batchCap;
    /* Multi-table mode: the entire file must fit in one buffer without any
     * mid-file flush (the ? placeholder SQL cannot be used with tbnames=NULL).
     * Use numRows directly, capped at STMT2_BATCH_MAX to bound memory per slot. */
    if (ctx->multiTable) {
        int64_t mtCap = (numRows < (int64_t)STMT2_BATCH_MAX) ? numRows : (int64_t)STMT2_BATCH_MAX;
        if (targetCap < mtCap) targetCap = mtCap;
    }
    /* The buffer must be large enough to hold at least one complete raw TDengine
     * block (≤ STMT2_MIN_ROW_BUF_CAP rows).  When --data-batch is smaller than
     * that, stmt2AccBlock uses batchCap as the flush trigger instead; the buffer
     * still needs the larger capacity so that a whole block can be assembled
     * before the first flush opportunity. */
    {
        int64_t minCap = (numRows < STMT2_MIN_ROW_BUF_CAP) ? numRows : STMT2_MIN_ROW_BUF_CAP;
        if (targetCap < minCap) targetCap = minCap;
    }

    bool needAlloc = (ctx->colBinds == NULL ||
                      ctx->colBindsCap < numCols ||
                      ctx->rowBufCap  < targetCap);

    /* Force re-allocation when a reused buffer slot changes between
     * fixed-size and variable-size types (e.g. INT → NCHAR across NTBs).
     * Variable types need a per-row `length` array that fixed types don't
     * allocate; reusing a fixed-type slot for a variable-type column would
     * leave `length` as NULL, causing a SIGSEGV in the STMT2 bind path. */
    if (!needAlloc && ctx->colBinds) {
        for (int i = 0; i < numCols; i++) {
            int backupColIdx = i;
            if (ctx->stbChange && ctx->stbChange->schemaChanged && ctx->stbChange->colMappings &&
                i < ctx->stbChange->matchColCount) {
                backupColIdx = ctx->stbChange->colMappings[i].backupIdx;
            }
            FieldInfo       *fi   = &fieldInfos[backupColIdx];
            TAOS_STMT2_BIND *bind = &ctx->colBinds[i];
            bool oldNeedsLen = IS_VAR_DATA_TYPE(bind->buffer_type) || IS_DECIMAL_TYPE(bind->buffer_type);
            bool newNeedsLen = IS_VAR_DATA_TYPE(fi->type) || IS_DECIMAL_TYPE(fi->type);
            if (oldNeedsLen != newNeedsLen || fi->type != bind->buffer_type) {
                needAlloc = true;
                break;
            }
        }
    }

    /* Space-for-time: before a fresh alloc, check if the spare buffer set from
     * the previous flush cycle can satisfy the new requirements (same or more
     * columns, same or larger row capacity).  Reclaiming the spare avoids the
     * per-file malloc+free loop in multi-table mode. */
    if (needAlloc && ctx->spareColBinds != NULL &&
        ctx->spareColBindsCap >= numCols && ctx->spareRowBufCap >= targetCap) {
        /* Detach spare → ctx.  The spare was fully reset on flush (accRows=0,
         * varWriteOffsets zeroed) so it can be used directly after the length
         * memset below. */
        ctx->colBinds        = ctx->spareColBinds;        ctx->spareColBinds        = NULL;
        ctx->colBindsCap     = ctx->spareColBindsCap;     ctx->spareColBindsCap     = 0;
        ctx->rowBufCap       = ctx->spareRowBufCap;       ctx->spareRowBufCap       = 0;
        ctx->varWriteOffsets = ctx->spareVarWriteOffsets; ctx->spareVarWriteOffsets = NULL;
        ctx->varBufCapacity  = ctx->spareVarBufCapacity;  ctx->spareVarBufCapacity  = NULL;
        needAlloc = false;
    }

    if (!needAlloc) {        ctx->accRows   = 0;
        ctx->numFields = numCols;
        ctx->fieldInfos = fieldInfos;
        /* reset packed write offsets for all variable-type columns */
        if (ctx->varWriteOffsets)
            memset(ctx->varWriteOffsets, 0, ctx->colBindsCap * sizeof(int32_t));
        /* also reset per-column length arrays for var/decimal columns:
         * zero only the rows that will actually be used, capped at rowBufCap
         * (the allocated length).  numRows can exceed rowBufCap when the file
         * has more rows than the batch capacity, causing a heap overflow if
         * numRows is used directly. */
        int64_t zeroRows = (numRows < (int64_t)ctx->rowBufCap) ? numRows : (int64_t)ctx->rowBufCap;
        for (int i = 0; i < numCols; i++) {
            /* Update buffer_type: when this buffer is reused between tables with
             * different schemas (e.g. NTB-0 has INT col, NTB-1 has FLOAT col),
             * the old buffer_type from the previous table must be overwritten. */
            int backupColIdx = i;
            if (ctx->stbChange && ctx->stbChange->schemaChanged && ctx->stbChange->colMappings &&
                i < ctx->stbChange->matchColCount) {
                backupColIdx = ctx->stbChange->colMappings[i].backupIdx;
            }
            ctx->colBinds[i].buffer_type = fieldInfos[backupColIdx].type;
            TAOS_STMT2_BIND *bind = &ctx->colBinds[i];
            if (bind->length) memset(bind->length, 0, (size_t)zeroRows * sizeof(int32_t));
        }
        return TSDB_CODE_SUCCESS;
    }

    /* free old buffers */
    if (ctx->colBinds) {
        stmt2FreeColBuffers(ctx);
        taosMemoryFree(ctx->colBinds);
        ctx->colBinds = NULL;
    }

    int64_t cap = targetCap;  /* already clamped to effective batchCap */
    ctx->colBinds = (TAOS_STMT2_BIND *)taosMemoryCalloc(numCols, sizeof(TAOS_STMT2_BIND));
    if (!ctx->colBinds) return TSDB_CODE_BCK_MALLOC_FAILED;

    ctx->varWriteOffsets = (int32_t *)taosMemoryCalloc(numCols, sizeof(int32_t));
    if (!ctx->varWriteOffsets) {
        taosMemoryFree(ctx->colBinds); ctx->colBinds = NULL;
        return TSDB_CODE_BCK_MALLOC_FAILED;
    }
    ctx->varBufCapacity = (int32_t *)taosMemoryCalloc(numCols, sizeof(int32_t));
    if (!ctx->varBufCapacity) {
        taosMemoryFree(ctx->varWriteOffsets); ctx->varWriteOffsets = NULL;
        taosMemoryFree(ctx->colBinds); ctx->colBinds = NULL;
        return TSDB_CODE_BCK_MALLOC_FAILED;
    }

    for (int i = 0; i < numCols; i++) {
        /* When schema has changed, bind position i corresponds to the i-th
         * matched column, whose actual backup index is colMappings[i].backupIdx.
         * We MUST use that backup field's type/bytes for allocation, not the
         * sequential fieldInfos[i] (which may be a completely different column). */
        int backupColIdx = i;
        if (ctx->stbChange && ctx->stbChange->schemaChanged && ctx->stbChange->colMappings &&
            i < ctx->stbChange->matchColCount) {
            backupColIdx = ctx->stbChange->colMappings[i].backupIdx;
        }
        FieldInfo       *fi   = &fieldInfos[backupColIdx];
        TAOS_STMT2_BIND *bind = &ctx->colBinds[i];
        bind->buffer_type = fi->type;
        if (IS_VAR_DATA_TYPE(fi->type)) {
            /* Cap initial allocation to avoid huge allocs for BLOB/MEDIUMBLOB.
             * Use dynamic realloc in stmt2AccBlock if data exceeds initial cap. */
            int64_t stride = (int64_t)fi->bytes;
            int64_t initialBufSize = cap * stride;
            if (initialBufSize > (int64_t)32 * 1024 * 1024) initialBufSize = (int64_t)32 * 1024 * 1024;
            if (initialBufSize < 4096) initialBufSize = 4096;
            bind->buffer   = taosMemoryCalloc(1, (size_t)initialBufSize);
            bind->length   = (int32_t *)taosMemoryCalloc(cap, sizeof(int32_t));
            ctx->varBufCapacity[i] = (int32_t)initialBufSize;
        } else if (IS_DECIMAL_TYPE(fi->type)) {
            /* DECIMAL is bound to STMT2 as a packed decimal string (same packed
             * layout as var-data).  precision/scale decoded from encoded fi->bytes. */
            uint8_t precision = fieldGetPrecision(fi);
            int32_t maxStrLen = (precision > 0 ? (int32_t)precision : 38) + 10;
            int32_t totalBufSize = (int32_t)cap * maxStrLen;
            bind->buffer   = taosMemoryCalloc(1, totalBufSize);
            bind->length   = (int32_t *)taosMemoryCalloc(cap, sizeof(int32_t));
            ctx->varBufCapacity[i] = totalBufSize;
        } else {
            int32_t typeBytes = tDataTypes[fi->type].bytes;
            /* Space-for-time: malloc (not calloc) — stmt2AccBlock performs memcpy
             * which fully overwrites the used region before it is read by STMT2. */
            bind->buffer   = taosMemoryMalloc((size_t)cap * typeBytes);
            bind->length   = NULL;
            ctx->varBufCapacity[i] = 0;
        }
        /* Space-for-time: use malloc (not calloc) for is_null — stmt2AccBlock
         * performs a per-block memset + sparse bitmap scan which initialises
         * every used row before it is read by STMT2. */
        bind->is_null = (char *)taosMemoryMalloc(cap);
        if (!bind->buffer || !bind->is_null ||
            ((IS_VAR_DATA_TYPE(fi->type) || IS_DECIMAL_TYPE(fi->type)) && !bind->length)) {
            stmt2FreeColBuffers(ctx);
            taosMemoryFree(ctx->colBinds); ctx->colBinds = NULL;
            return TSDB_CODE_BCK_MALLOC_FAILED;
        }
    }
    ctx->colBindsCap = numCols;
    ctx->rowBufCap   = cap;
    ctx->numFields   = numCols;
    ctx->fieldInfos  = fieldInfos;
    ctx->accRows     = 0;
    return TSDB_CODE_SUCCESS;
}

//
// Prepare STMT2 once per thread:
//   INSERT INTO ? VALUES(?,?,?...)
// With singleStbInsert + singleTableBindOnce enabled for maximum server-side
// throughput (no per-call schema re-negotiation).
//
static int stmt2PrepareOnce(Stmt2RestoreCtx *ctx, int numCols, FieldInfo *fieldInfos) {
    /* build SQL — use fully qualified table name so WebSocket mode can resolve
     * the schema without needing a prior taos_select_db or a ? placeholder.
     * Native mode also handles this correctly (local metadata lookup). */
    const char *partCols  = "";
    int         bindCount = numCols;
    if (ctx->stbChange && ctx->stbChange->schemaChanged && ctx->stbChange->partColsStr) {
        partCols   = ctx->stbChange->partColsStr;
        bindCount  = ctx->stbChange->matchColCount;
        logInfo("stmt2 partial column write: %s (bind %d of %d cols)", partCols, bindCount, numCols);
    }

    char *sql = (char *)taosMemoryMalloc(TSDB_MAX_SQL_LEN);
    if (!sql) return TSDB_CODE_BCK_MALLOC_FAILED;
    char *p = sql;
    /* Use db.tablename directly — avoids WebSocket STMT2 prepare failure
     * ("Table does not exist") that occurs with INSERT INTO ? over WebSocket
     * because the Rust WebSocket library validates the table at prepare time. */
    p += snprintf(p, TSDB_MAX_SQL_LEN, "INSERT INTO `%s`.`%s` %s VALUES(?",
                  ctx->dbName, ctx->tbName, partCols);
    for (int i = 1; i < bindCount; i++) p += sprintf(p, ",?");
    sprintf(p, ")");
    logDebug("stmt2 prepare: %s", sql);

    /* In WebSocket/DSN mode (multiTable=false) the same handle is always
     * singleTableBindOnce=true and can be reused across files — just call
     * taos_stmt2_prepare again with the new table-specific SQL instead of
     * closing and re-initialising the handle.
     * taosBenchmark's autoTblCreating path does the same: it calls
     * taos_stmt2_prepare once per child-table on the same handle.
     *
     * In native multi-table mode (multiTable=true) the caller already closed
     * the multi-table handle (different singleTableBindOnce option), so
     * ctx->stmt2 will be NULL here and we init a fresh single-table handle. */
    if (!ctx->stmt2) {
        ctx->stmt2 = initStmt2(ctx->conn, true);   /* singleTableBindOnce=true */
        if (!ctx->stmt2) {
            logError("initStmt2 failed for %s", ctx->dbName);
            taosMemoryFree(sql);
            return TSDB_CODE_BCK_STMT_FAILED;
        }
    }

    int ret = taos_stmt2_prepare(ctx->stmt2, sql, 0);
    taosMemoryFree(sql);
    if (ret != 0) {
        logError("taos_stmt2_prepare failed: %s", taos_stmt2_error(ctx->stmt2));
        taos_stmt2_close(ctx->stmt2); ctx->stmt2 = NULL;
        return ret;
    }

    ctx->prepared = true;
    (void)fieldInfos;   /* used only for bind buffer alloc, not here */
    return TSDB_CODE_SUCCESS;
}

/* forward declaration: stmt2ResetOnError calls stmt2FreeSlot (defined below) */
void stmt2FreeSlot(Stmt2TableSlot *slot);

//
// Close and re-initialise STMT2 after a failure.
// Sets prepared=false so the next file triggers stmt2PrepareOnce().
//
bool stmt2ResetOnError(Stmt2RestoreCtx *ctx) {
    if (ctx->stmt2) { taos_stmt2_close(ctx->stmt2); ctx->stmt2 = NULL; }
    ctx->prepared  = false;
    ctx->accRows   = 0;
    /* discard pending multi-table slots — they may contain partial data from a
     * failed state; flushing them risks duplicate or corrupt rows */
    if (ctx->numPending > 0) {
        for (int i = 0; i < ctx->numPending; i++) stmt2FreeSlot(&ctx->pendingSlots[i]);
        ctx->numPending       = 0;
        ctx->totalPendingRows = 0;
    }
    /* discard spare to avoid reusing potentially stale buffers after an error */
    if (ctx->spareColBinds) {
        for (int i = 0; i < ctx->spareColBindsCap; i++) {
            TAOS_STMT2_BIND *b = &ctx->spareColBinds[i];
            taosMemoryFree(b->buffer);  b->buffer  = NULL;
            taosMemoryFree(b->length);  b->length  = NULL;
            taosMemoryFree(b->is_null); b->is_null = NULL;
        }
        taosMemoryFree(ctx->spareColBinds);        ctx->spareColBinds        = NULL;
        taosMemoryFree(ctx->spareVarWriteOffsets); ctx->spareVarWriteOffsets = NULL;
        taosMemoryFree(ctx->spareVarBufCapacity);  ctx->spareVarBufCapacity  = NULL;
        ctx->spareColBindsCap = 0;
        ctx->spareRowBufCap   = 0;
    }
    return true;   /* caller will retry prepare on next file */
}

/* forward declaration: stmt2AccBlock calls stmt2FlushBatch (defined below) */
static int stmt2FlushBatch(Stmt2RestoreCtx *ctx);

//
// Accumulate one raw block into the per-column stride buffers.
// Called per data block by dataBlockCallbackV2.
//
static int stmt2AccBlock(Stmt2RestoreCtx *ctx, void *blockData, int32_t blockRows,
                         FieldInfo *fieldInfos, int numFields) {
    /* Flush the current batch before accumulating this block when:
     *   (a) the block would overflow the row buffer, OR
     *   (b) the user's --data-batch threshold has already been reached.
     * Case (b) is needed when --data-batch < STMT2_MIN_ROW_BUF_CAP: the buffer
     * can still hold more rows, but we honour the user's requested batch size.
     * In multi-table mode both triggers are suppressed: the buffer is sized to
     * hold all rows of a single file (numRows ≤ STMT2_BATCH_MAX), so overflow
     * cannot occur, and batchThresh flushing would call stmt2FlushBatch with
     * tbnames=NULL which is incompatible with the ? placeholder SQL. */
    int64_t batchThresh = (ctx->multiTable) ? ctx->rowBufCap
                        : (argDataBatch() > 0) ? (int64_t)argDataBatch() : ctx->rowBufCap;
    if (ctx->accRows + blockRows > ctx->rowBufCap ||
        (ctx->accRows > 0 && ctx->accRows >= batchThresh)) {
        int flushCode = stmt2FlushBatch(ctx);
        if (flushCode != TSDB_CODE_SUCCESS) return flushCode;
    }

    BlockReader reader;
    int code = initBlockReader(&reader, blockData);
    if (code != TSDB_CODE_SUCCESS) return code;

    int bindColCount = ctx->numFields; /* may differ from numFields if schema change */
    int bindIdx = 0;
    for (int c = 0; c < numFields; c++) {
        /* check schema-change skip via bindIdxMap */
        int bi = getPartialWriteBindIdx(
            (ctx->stbChange && ctx->stbChange->schemaChanged) ? ctx->stbChange : NULL,
            c);
        if (bi < 0) {
            void *sd = NULL; int32_t sl = 0;
            getColumnData(&reader, fieldInfos[c].type, &sd, &sl);
            continue;
        }
        if (bindIdx >= bindColCount) break;

        void *colData = NULL; int32_t colDataLen = 0;
        code = getColumnData(&reader, fieldInfos[c].type, &colData, &colDataLen);
        if (code != TSDB_CODE_SUCCESS) return code;

        TAOS_STMT2_BIND *bind = &ctx->colBinds[bindIdx];
        int64_t base = ctx->accRows;

        if (IS_VAR_DATA_TYPE(fieldInfos[c].type)) {
            int32_t *offsets    = (int32_t *)colData;
            char    *varDataBase = (char *)colData + blockRows * sizeof(int32_t);
            bool     isNchar    = (fieldInfos[c].type == TSDB_DATA_TYPE_NCHAR);
            for (int row = 0; row < blockRows; row++) {
                if (offsets[row] < 0) {
                    bind->is_null[base + row] = 1;
                    if (bind->length) bind->length[base + row] = 0;
                } else {
                    bind->is_null[base + row] = 0;
                    uint16_t varLen = *(uint16_t *)(varDataBase + offsets[row]);
                    char    *src    = varDataBase + offsets[row] + sizeof(uint16_t);
                    // Write packed: each row's data immediately follows the previous
                    int32_t writeOff = ctx->varWriteOffsets[bindIdx];
                    if (isNchar && varLen > 0) {
                        /* UTF-8 can be at most equal in bytes to UCS-4 size */
                        int32_t maxUtf8 = varLen;
                        /* ensure buffer has room */
                        if (writeOff + maxUtf8 > ctx->varBufCapacity[bindIdx]) {
                            int32_t newCap = (writeOff + maxUtf8) * 2;
                            if (newCap < ctx->varBufCapacity[bindIdx] * 2) newCap = ctx->varBufCapacity[bindIdx] * 2;
                            void *newBuf = taosMemoryRealloc(bind->buffer, newCap);
                            if (!newBuf) return TSDB_CODE_BCK_MALLOC_FAILED;
                            bind->buffer = newBuf;
                            ctx->varBufCapacity[bindIdx] = newCap;
                        }
                        int32_t utf8Len = taosUcs4ToMbs(
                            (TdUcs4 *)src, varLen,
                            (char *)bind->buffer + writeOff, NULL);
                        int32_t actualLen = utf8Len > 0 ? utf8Len : 0;
                        if (bind->length) bind->length[base + row] = actualLen;
                        ctx->varWriteOffsets[bindIdx] += actualLen;
                    } else {
                        int32_t stride = fieldInfos[c].bytes;
                        int32_t cp = (varLen < (uint16_t)stride) ? varLen : (uint16_t)stride;
                        /* ensure buffer has room (handles BLOB/MEDIUMBLOB large data) */
                        if (writeOff + cp > ctx->varBufCapacity[bindIdx]) {
                            int32_t newCap = (writeOff + cp) * 2;
                            if (newCap < ctx->varBufCapacity[bindIdx] * 2) newCap = ctx->varBufCapacity[bindIdx] * 2;
                            void *newBuf = taosMemoryRealloc(bind->buffer, newCap);
                            if (!newBuf) return TSDB_CODE_BCK_MALLOC_FAILED;
                            bind->buffer = newBuf;
                            ctx->varBufCapacity[bindIdx] = newCap;
                        }
                        memcpy((char *)bind->buffer + writeOff, src, cp);
                        if (bind->length) bind->length[base + row] = cp;
                        ctx->varWriteOffsets[bindIdx] += cp;
                    }
                }
            }
        } else if (IS_DECIMAL_TYPE(fieldInfos[c].type)) {
            /* DECIMAL: raw block has bitmap + fixed bytes (8 or 16) per row.
             * STMT2 for DECIMAL expects packed decimal string representation. */
            int32_t actualBytes = fieldGetRawBytes(&fieldInfos[c]);
            uint8_t precision   = fieldGetPrecision(&fieldInfos[c]);
            uint8_t scale       = fieldGetScale(&fieldInfos[c]);
            int32_t bitmapLen   = (blockRows + 7) / 8;
            char   *bitmap      = (char *)colData;
            char   *fixData     = (char *)colData + bitmapLen;
            for (int row = 0; row < blockRows; row++) {
                bind->is_null[base + row] =
                    (bitmap[row >> 3] & (1u << (7 - (row & 7)))) ? 1 : 0;
                if (!bind->is_null[base + row]) {
                    void   *rawDec   = fixData + row * actualBytes;
                    char    str[64]  = "";
                    decimalToStr(rawDec, fieldInfos[c].type, precision, scale, str, sizeof(str));
                    int32_t slen     = (int32_t)strlen(str);
                    int32_t writeOff = ctx->varWriteOffsets[bindIdx];
                    /* ensure buffer has room */
                    if (writeOff + slen > ctx->varBufCapacity[bindIdx]) {
                        int32_t newCap = (writeOff + slen) * 2;
                        if (newCap < ctx->varBufCapacity[bindIdx] * 2) newCap = ctx->varBufCapacity[bindIdx] * 2;
                        void *newBuf = taosMemoryRealloc(bind->buffer, newCap);
                        if (!newBuf) return TSDB_CODE_BCK_MALLOC_FAILED;
                        bind->buffer = newBuf;
                        ctx->varBufCapacity[bindIdx] = newCap;
                    }
                    memcpy((char *)bind->buffer + writeOff, str, slen);
                    bind->length[base + row] = slen;
                    ctx->varWriteOffsets[bindIdx] += slen;
                } else {
                    bind->length[base + row] = 0;
                }
            }
        } else {
            /* Fixed type: copy data in one shot, then build is_null with
             * space-for-time sparse bitmap scan (memset zero + mark only nulls).
             * TDengine bitmap: bit7=row0 (MSB-first within each byte).
             * Skipping zero bitmap bytes avoids any per-row work when data is
             * entirely non-null (the common case for most time-series workloads). */
            int32_t typeBytes = tDataTypes[fieldInfos[c].type].bytes;
            int32_t bitmapLen = (blockRows + 7) / 8;
            char   *bitmap    = (char *)colData;
            char   *fixData   = (char *)colData + bitmapLen;
            memcpy((char *)bind->buffer + base * typeBytes, fixData, (size_t)blockRows * typeBytes);
            memset(bind->is_null + base, 0, blockRows);
            for (int bi = 0; bi < bitmapLen; bi++) {
                uint8_t byt = (uint8_t)bitmap[bi];
                if (!byt) continue;  /* fast path: no nulls in this 8-row group */
                int base8 = bi * 8;
                for (int bit = 7; bit >= 0; bit--) {
                    int row = base8 + (7 - bit);
                    if (row >= blockRows) break;
                    if (byt & (1u << bit)) bind->is_null[base + row] = 1;
                }
            }
        }
        bindIdx++;
    }
    ctx->accRows += blockRows;
    return TSDB_CODE_SUCCESS;
}

typedef struct { Stmt2RestoreCtx *ctx; FieldInfo *fis; int nf; } S2CallbackData;

static int dataBlockCallbackV2(void *userData, FieldInfo *fieldInfos,
                               int numFields, void *blockData,
                               int32_t blockLen, int32_t blockRows) {
    S2CallbackData *d = (S2CallbackData *)userData;
    (void)blockLen;
    if (g_interrupted) return TSDB_CODE_BCK_USER_CANCEL;
    if (blockRows == 0) return TSDB_CODE_SUCCESS;
    return stmt2AccBlock(d->ctx, blockData, blockRows, d->fis, d->nf);
}

//
// Bind the currently accumulated rows and execute, then reset accRows to 0.
// Called both mid-accumulation (overflow guard) and at end-of-file.
//
static int stmt2FlushBatch(Stmt2RestoreCtx *ctx) {
    if (ctx->accRows == 0) return TSDB_CODE_SUCCESS;

    /* set row count on each column */
    for (int i = 0; i < ctx->numFields; i++)
        ctx->colBinds[i].num = (int)ctx->accRows;

    /* Table name is embedded in the prepare SQL (INSERT INTO db.tbl VALUES(?))
     * so tbnames must be NULL — passing it would override the SQL table name. */
    TAOS_STMT2_BINDV bindv;
    memset(&bindv, 0, sizeof(bindv));
    bindv.count     = 1;
    bindv.tbnames   = NULL;             /* name already in prepared SQL */
    bindv.tags      = NULL;             /* child table already created */
    bindv.bind_cols = &ctx->colBinds;   /* [0] = column array for 1st table */

    int code = taos_stmt2_bind_param(ctx->stmt2, &bindv, -1);
    if (code != 0) {
        logError("taos_stmt2_bind_param failed (%s.%s): %s",
                 ctx->dbName, ctx->tbName, taos_stmt2_error(ctx->stmt2));
        return code;
    }

    int affectedRows = 0;
    code = taos_stmt2_exec(ctx->stmt2, &affectedRows);
    if (code != 0) {
        logError("taos_stmt2_exec failed (%s.%s): %s",
                 ctx->dbName, ctx->tbName, taos_stmt2_error(ctx->stmt2));
        return code;
    }

    ctx->totalRows += affectedRows;
    ctx->accRows    = 0;
    /* reset packed write offsets for variable-type columns */
    if (ctx->varWriteOffsets)
        memset(ctx->varWriteOffsets, 0, ctx->colBindsCap * sizeof(int32_t));
    return TSDB_CODE_SUCCESS;
}

//
// Execute the accumulated rows for the current file using STMT2.
// Delegates to stmt2FlushBatch which also handles mid-file overflow flushes.
//
static int stmt2ExecFile(Stmt2RestoreCtx *ctx) {
    return stmt2FlushBatch(ctx);
}

// =====================================================================
//  STMT2 multi-table batch helpers  (native mode, STB CTBs only)
// =====================================================================

//
// Free all buffers in one pending slot (does not zero the slot itself for
// compatibility with memset-based cleanup in stmt2FlushMultiTableSlots).
//
void stmt2FreeSlot(Stmt2TableSlot *slot) {
    if (!slot->colBinds) return;
    for (int c = 0; c < slot->colBindsCap; c++) {
        TAOS_STMT2_BIND *b = &slot->colBinds[c];
        taosMemoryFree(b->buffer);  b->buffer  = NULL;
        taosMemoryFree(b->length);  b->length  = NULL;
        taosMemoryFree(b->is_null); b->is_null = NULL;
    }
    taosMemoryFree(slot->varWriteOffsets); slot->varWriteOffsets = NULL;
    taosMemoryFree(slot->varBufCapacity);  slot->varBufCapacity  = NULL;
    taosMemoryFree(slot->colBinds);        slot->colBinds        = NULL;
    memset(slot, 0, sizeof(*slot));
}

//
// Prepare STMT2 once for the whole thread using INSERT INTO ? VALUES(?,...)
// with singleStbInsert=true, singleTableBindOnce=false (multi-table mode).
// This is used for native connections only; WebSocket/DSN falls back to the
// per-file approach (INSERT INTO db.tbl VALUES(?)).
//
static int stmt2PrepareOnceMulti(Stmt2RestoreCtx *ctx, int numCols,
                                  FieldInfo *fieldInfos) {
    const char *partCols  = "";
    int         bindCount = numCols;
    if (ctx->stbChange && ctx->stbChange->schemaChanged && ctx->stbChange->partColsStr) {
        partCols   = ctx->stbChange->partColsStr;
        bindCount  = ctx->stbChange->matchColCount;
        logInfo("stmt2 multi-table partial column write: %s (bind %d of %d cols)",
                partCols, bindCount, numCols);
    }

    char *sql = (char *)taosMemoryMalloc(TSDB_MAX_SQL_LEN);
    if (!sql) return TSDB_CODE_BCK_MALLOC_FAILED;
    char *p = sql;
    p += snprintf(p, TSDB_MAX_SQL_LEN, "INSERT INTO ? %s VALUES(?", partCols);
    for (int i = 1; i < bindCount; i++) p += sprintf(p, ",?");
    sprintf(p, ")");
    logDebug("stmt2 multi-table prepare: %s", sql);

    if (ctx->stmt2) { taos_stmt2_close(ctx->stmt2); ctx->stmt2 = NULL; }

    /* singleTableBindOnce=false enables multi-table binding in one TAOS_STMT2_BINDV */
    TAOS_STMT2_OPTION op2;
    memset(&op2, 0, sizeof(op2));
    op2.singleStbInsert     = true;
    op2.singleTableBindOnce = false;
    ctx->stmt2 = taos_stmt2_init(ctx->conn, &op2);
    if (!ctx->stmt2) {
        logError("stmt2 multi-table init failed for %s", ctx->dbName);
        taosMemoryFree(sql);
        return TSDB_CODE_BCK_STMT_FAILED;
    }

    int ret = taos_stmt2_prepare(ctx->stmt2, sql, 0);
    taosMemoryFree(sql);
    if (ret != 0) {
        logError("stmt2 multi-table prepare failed: %s", taos_stmt2_error(ctx->stmt2));
        taos_stmt2_close(ctx->stmt2); ctx->stmt2 = NULL;
        return ret;
    }
    ctx->prepared = true;
    (void)fieldInfos;
    return TSDB_CODE_SUCCESS;
}

//
// Seal: transfer ctx->colBinds ownership into a new pending slot, then reset
// ctx so that the next stmt2AllocColBuffers call allocates fresh buffers.
//
static void stmt2SealSlot(Stmt2RestoreCtx *ctx, const char *fqn) {
    Stmt2TableSlot *slot = &ctx->pendingSlots[ctx->numPending];
    strncpy(slot->fqn, fqn, sizeof(slot->fqn) - 1);
    slot->fqn[sizeof(slot->fqn) - 1] = '\0';

    /* Transfer buffer ownership: ctx → slot */
    slot->colBinds        = ctx->colBinds;
    slot->colBindsCap     = ctx->colBindsCap;
    slot->numCols         = ctx->numFields;
    slot->numRows         = (int32_t)ctx->accRows;
    slot->rowBufCap       = ctx->rowBufCap;   /* save capacity for spare reuse check */
    slot->varWriteOffsets = ctx->varWriteOffsets;
    slot->varBufCapacity  = ctx->varBufCapacity;

    /* Detach from ctx so the next alloc creates fresh per-slot buffers */
    ctx->colBinds        = NULL;
    ctx->colBindsCap     = 0;
    ctx->rowBufCap       = 0;
    ctx->accRows         = 0;
    ctx->varWriteOffsets = NULL;
    ctx->varBufCapacity  = NULL;

    ctx->totalPendingRows += slot->numRows;
    ctx->numPending++;
}

//
// Flush all pending slots as a single multi-table TAOS_STMT2_BINDV call
// (one taos_stmt2_bind_param + one taos_stmt2_exec for N child tables).
// Frees all slot buffers after the call regardless of success/failure.
//
int stmt2FlushMultiTableSlots(Stmt2RestoreCtx *ctx) {
    if (ctx->numPending == 0) return TSDB_CODE_SUCCESS;

    int N = ctx->numPending;
    char           **tbnames  = (char **)taosMemoryCalloc(N, sizeof(char *));
    TAOS_STMT2_BIND **bindcols = (TAOS_STMT2_BIND **)taosMemoryCalloc(N, sizeof(TAOS_STMT2_BIND *));
    if (!tbnames || !bindcols) {
        taosMemoryFree(tbnames);
        taosMemoryFree(bindcols);
        for (int i = 0; i < N; i++) stmt2FreeSlot(&ctx->pendingSlots[i]);
        ctx->numPending = 0; ctx->totalPendingRows = 0;
        return TSDB_CODE_BCK_MALLOC_FAILED;
    }

    for (int i = 0; i < N; i++) {
        Stmt2TableSlot *slot = &ctx->pendingSlots[i];
        /* set per-row-count on each column bind of this slot */
        for (int c = 0; c < slot->numCols; c++)
            slot->colBinds[c].num = slot->numRows;
        tbnames[i]  = slot->fqn;
        bindcols[i] = slot->colBinds;
    }

    TAOS_STMT2_BINDV bindv;
    memset(&bindv, 0, sizeof(bindv));
    bindv.count     = N;
    bindv.tbnames   = tbnames;
    bindv.tags      = NULL;
    bindv.bind_cols = bindcols;

    int code = taos_stmt2_bind_param(ctx->stmt2, &bindv, -1);
    taosMemoryFree(tbnames);
    taosMemoryFree(bindcols);

    if (code != 0) {
        logError("stmt2 multi-table bind_param failed (%s, %d tables): %s",
                 ctx->dbName, N, taos_stmt2_error(ctx->stmt2));
        goto cleanup;
    }

    {
        int affectedRows = 0;
        code = taos_stmt2_exec(ctx->stmt2, &affectedRows);
        if (code != 0) {
            logError("stmt2 multi-table exec failed (%s, %d tables): %s",
                     ctx->dbName, N, taos_stmt2_error(ctx->stmt2));
        } else {
            ctx->totalRows += affectedRows;
            logDebug("stmt2 multi-table flush: %d tables, %d rows", N, affectedRows);

            /* Space-for-time: before freeing all slots, detach slot[0]'s column
             * buffers as a spare set.  stmt2AllocColBuffers will reclaim this spare
             * for the next batch instead of doing a fresh malloc+free cycle. */
            if (ctx->spareColBinds == NULL && N > 0) {
                Stmt2TableSlot *s0 = &ctx->pendingSlots[0];
                /* Reset variable-column write offsets so the spare is ready to use */
                if (s0->varWriteOffsets)
                    memset(s0->varWriteOffsets, 0, s0->colBindsCap * sizeof(int32_t));
                ctx->spareColBinds        = s0->colBinds;        s0->colBinds        = NULL;
                ctx->spareColBindsCap     = s0->colBindsCap;
                ctx->spareRowBufCap       = s0->rowBufCap;
                ctx->spareVarWriteOffsets = s0->varWriteOffsets; s0->varWriteOffsets = NULL;
                ctx->spareVarBufCapacity  = s0->varBufCapacity;  s0->varBufCapacity  = NULL;
            }
        }
    }

cleanup:
    for (int i = 0; i < N; i++) stmt2FreeSlot(&ctx->pendingSlots[i]);
    ctx->numPending       = 0;
    ctx->totalPendingRows = 0;
    return code;
}

//
// Restore one .dat file using TAOS_STMT2.
//
// Two paths depending on ctx->multiTable (set once per thread by restoreDataThread):
//
//   MULTI-TABLE (native, STB CTBs, numRows <= STMT2_BATCH_MAX):
//     Prepare INSERT INTO ? VALUES(?,...)  once per thread.
//     Read file into ctx->colBinds, seal into a pending slot.
//     Flush TAOS_STMT2_BINDV{count=N, tbnames[], bind_cols[]} when
//     numPending == STMT2_MULTI_TABLE_PENDING or totalPendingRows >= batchCap.
//     Final flush happens in restoreDataThread after the file loop.
//
//   SINGLE-TABLE (WebSocket/DSN, NTB, or large file > STMT2_BATCH_MAX rows):
//     Original per-file: close/init/prepare with embedded table name, exec immediately.
//
int restoreOneDataFileV2(Stmt2RestoreCtx *ctx, const char *filePath) {
    /* extract table name */
    const char *base = strrchr(filePath, '/');
    if (base) base++; else base = filePath;
    char tbName[TSDB_TABLE_NAME_LEN] = {0};
    const char *dot = strrchr(base, '.');
    if (dot && (dot - base) < (int)sizeof(tbName))
        memcpy(tbName, base, dot - base);
    else
        snprintf(tbName, sizeof(tbName), "%s", base);

    bool isNtb = (strstr(filePath, "/_ntb_data") != NULL);

    int code = TSDB_CODE_SUCCESS;
    TaosFile *f = openTaosFileForRead(filePath, &code);
    if (!f) { logError("open failed(%d): %s", code, filePath); return code; }
    if (f->header.numRows == 0) { closeTaosFileRead(f); return TSDB_CODE_SUCCESS; }

    ctx->lastFileSize = f->fileSize;  /* cache for caller's stat elimination */

    strncpy(ctx->tbName, tbName, TSDB_TABLE_NAME_LEN - 1);
    int       numCols = f->header.numFields;
    FieldInfo *fis    = (FieldInfo *)f->header.schema;
    int64_t   numRows = f->header.numRows;

    /* Per-table NTB schema change detection */
    StbChange *localStbChange = NULL;
    if (ctx->stbChange == NULL && isNtb) {
        localStbChange = buildNtbSchemaChange(ctx->conn, ctx->dbName, tbName,
                                              fis, f->header.numFields);
        if (localStbChange) ctx->stbChange = localStbChange;
    }

    if (ctx->stbChange && ctx->stbChange->schemaChanged)
        numCols = ctx->stbChange->matchColCount;

    /* Choose path: multi-table only for native, STB CTBs, and files that fit
     * entirely within STMT2_BATCH_MAX rows (avoids mid-file flush complications). */
    if (ctx->multiTable && !isNtb && numRows <= (int64_t)STMT2_BATCH_MAX) {
        /* ---- MULTI-TABLE PATH ---- */

        /* One-time prepare: INSERT INTO ? VALUES(?,...)  reused for all files */
        if (!ctx->prepared) {
            /* Select the target database so that the multi-table tbnames look-up
             * can resolve plain table names (no db prefix required).
             * Backtick-quote the name so that Chinese / special-char db names
             * are accepted by taos_select_db (unquoted names fail with
             * TSDB_CODE_PAR_SYNTAX_ERROR for non-ASCII identifiers). */
            char quotedDb[TSDB_DB_NAME_LEN + 4];
            snprintf(quotedDb, sizeof(quotedDb), "`%s`", ctx->dbName);
            int selRet = taos_select_db(ctx->conn, quotedDb);
            if (selRet != 0)
                logWarn("stmt2 multi-table: taos_select_db(%s) failed (code=%d), continuing",
                        ctx->dbName, selRet);
            code = stmt2PrepareOnceMulti(ctx, numCols, fis);
            if (code != TSDB_CODE_SUCCESS) { closeTaosFileRead(f); goto done; }
        }

        /* Allocate per-column buffers for this file (buffer sized to numRows,
         * bypassing the batchCap clamp because multiTable is true). */
        code = stmt2AllocColBuffers(ctx, numRows, numCols, fis);
        if (code != TSDB_CODE_SUCCESS) { closeTaosFileRead(f); goto done; }

        /* Stream blocks into ctx->colBinds */
        S2CallbackData cbd = { ctx, fis, f->header.numFields };
        code = readTaosFileBlocks(f, dataBlockCallbackV2, &cbd);
        closeTaosFileRead(f);
        if (code != TSDB_CODE_SUCCESS) {
            logError("read blocks failed(%d): %s", code, filePath);
            goto done;
        }

        if (ctx->accRows == 0) goto done;  /* file became empty after filtering */

        /* Seal: transfer buffer ownership from ctx into a new pending slot */
        char fqn[TSDB_DB_NAME_LEN + TSDB_TABLE_NAME_LEN + 8];
        /* Use plain table name (no db prefix): taos_select_db has already set
         * the default database for this connection so tbnames resolution works. */
        snprintf(fqn, sizeof(fqn), "%s", tbName);
        stmt2SealSlot(ctx, fqn);

        /* Flush if batch thresholds are reached */
        int64_t batchCap = (argDataBatch() > 0) ? (int64_t)argDataBatch() : (int64_t)STMT2_BATCH_DEFAULT;
        if (ctx->numPending >= STMT2_MULTI_TABLE_PENDING ||
            ctx->totalPendingRows >= batchCap) {
            code = stmt2FlushMultiTableSlots(ctx);
            if (code != TSDB_CODE_SUCCESS)
                logError("stmt2 multi-table flush failed (%s): %d", ctx->dbName, code);
            else
                ctx->lastCallFlushed = true;  // signal restoreDataThread to drain pending checkpoints
        }

        logDebug("stmt2 multi-table sealed %s.%s rows: %" PRId64 " pending_slots: %d",
                 ctx->dbName, tbName, ctx->totalRows, ctx->numPending);
    } else {
        /* ---- SINGLE-TABLE PATH (WebSocket / NTB / large file) ---- */

        /* In native multi-table mode (multiTable=true) the current stmt2 handle
         * may be a multi-table one (singleTableBindOnce=false).  Close it so
         * stmt2PrepareOnce will init a new single-table handle.
         * In WebSocket/DSN mode (multiTable=false) the handle is always
         * single-table — reuse it by just calling prepare again, no close+init. */
        if (ctx->multiTable && ctx->stmt2) {
            taos_stmt2_close(ctx->stmt2);
            ctx->stmt2 = NULL;
        }
        code = stmt2PrepareOnce(ctx, numCols, fis);
        if (code != TSDB_CODE_SUCCESS) { closeTaosFileRead(f); goto done; }

        code = stmt2AllocColBuffers(ctx, numRows, numCols, fis);
        if (code != TSDB_CODE_SUCCESS) { closeTaosFileRead(f); goto done; }

        S2CallbackData cbd = { ctx, fis, f->header.numFields };
        code = readTaosFileBlocks(f, dataBlockCallbackV2, &cbd);
        closeTaosFileRead(f);
        if (code != TSDB_CODE_SUCCESS) {
            logError("read blocks failed(%d): %s", code, filePath);
            goto done;
        }

        code = stmt2ExecFile(ctx);
        if (code == TSDB_CODE_SUCCESS)
            logDebug("stmt2 restore done: %s.%s rows: %" PRId64, ctx->dbName, tbName, ctx->totalRows);
        else
            logError("stmt2 exec failed: %s.%s", ctx->dbName, tbName);

        /* Reset prepared flag so that a subsequent multi-table file (native mode)
         * re-establishes the multi-table handle (different singleTableBindOnce). */
        if (ctx->multiTable) ctx->prepared = false;
    }

done:
    if (localStbChange) {
        ctx->stbChange = NULL;
        freeStbChange(localStbChange);
    }
    return code;
}


//
// Restore a single .par (Parquet) file using STMT
//

//
// Restore a single .par (Parquet) file using STMT2.
//
// Mirrors restoreOneParquetFile() but uses the TAOS_STMT2 API so the parquet
// restore path honours the user's -v option just like binary files do.
//
int restoreOneParquetFileV2(TAOS_STMT2 **stmt2Ptr, TAOS *conn, const char *dbName,
                                    const char *filePath,
                                    StbChange *stbChange,
                                    int64_t   *rowsOut) {
    (void)stbChange;  /* reserved for future schema-change support */
    int code = TSDB_CODE_SUCCESS;

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

    logDebug("restore parquet file (STMT2): %s -> table: %s.%s", filePath, dbName, tbName);

    /* Open file briefly just to determine the column count for the SQL */
    int numFields = 0;
    {
        int            err = TSDB_CODE_FAILED;
        ParquetReader *pr  = parquetReaderOpen(filePath, &err);
        if (!pr) {
            logError("open parquet file failed(%d): %s", err, filePath);
            return err;
        }
        TAOS_FIELD *fields = NULL;
        numFields = parquetReaderGetFields(pr, &fields);
        parquetReaderClose(pr);
        if (numFields <= 0) {
            logError("parquet file has no fields: %s", filePath);
            return TSDB_CODE_BCK_NO_FIELDS;
        }
    }

    /* Build INSERT SQL with db.table fully qualified (required for WebSocket) */
    char *sql = (char *)taosMemoryMalloc(TSDB_MAX_SQL_LEN);
    if (!sql) return TSDB_CODE_BCK_MALLOC_FAILED;
    char *p = sql;
    p += snprintf(p, TSDB_MAX_SQL_LEN, "INSERT INTO `%s`.`%s` VALUES(?",
                  argRenameDb(dbName), tbName);
    for (int i = 1; i < numFields; i++) p += sprintf(p, ",?");
    sprintf(p, ")");
    logDebug("parquet stmt2 prepare: %s", sql);

    /* Init STMT2 once per thread; re-prepare for each new table (avoids repeated init+close) */
    if (!*stmt2Ptr) {
        *stmt2Ptr = initStmt2(conn, true);
        if (!*stmt2Ptr) {
            taosMemoryFree(sql);
            return TSDB_CODE_BCK_STMT_FAILED;
        }
    }
    TAOS_STMT2 *stmt2 = *stmt2Ptr;

    int ret = taos_stmt2_prepare(stmt2, sql, 0);
    taosMemoryFree(sql);
    if (ret != 0) {
        logError("taos_stmt2_prepare failed for parquet: %s", taos_stmt2_error(stmt2));
        // Invalidate handle so next file gets a fresh one
        taos_stmt2_close(stmt2);
        *stmt2Ptr = NULL;
        return ret;
    }

    int64_t rows = 0;
    code = fileParquetToStmt2(stmt2, filePath, &rows);
    if (code != TSDB_CODE_SUCCESS) {
        logError("restore parquet (STMT2) failed(%d): %s -> %s.%s",
                 code, filePath, dbName, tbName);
    } else {
        logDebug("restore parquet (STMT2) done: %s.%s rows: %" PRId64, dbName, tbName, rows);
        if (rowsOut) *rowsOut = rows;
    }

    // stmt2 is NOT closed here; the caller owns it and closes after all files are done
    return code;
}
