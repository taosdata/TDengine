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

#include "storageParquet.h"
#include "parquetBlock.h"
#include "colCompress.h"   /* fieldGetRawBytes / fieldGetPrecision / fieldGetScale */
#include "decimal.h"       /* decimalToStr — for STMT2 DECIMAL string conversion  */
#include "ttypes.h"        /* IS_DECIMAL_TYPE                                      */
#include "bckLog.h"
#include <taoserror.h>
#include <taos.h>

/* ── flush threshold: execute STMT after this many accumulated rows ── */
#define PAR_STMT_BATCH_THRESHOLD 50000

/* ─────────────────────────────── backup ────────────────────────────*/

/*
 * Write a TAOS_RES result set to a .par (Apache Arrow / Parquet) file.
 * Blocks are fetched with taos_fetch_raw_block and written as Parquet
 * row-groups via the Arrow C++ writer wrapped in parquetBlock.cpp.
 */
int resultToFileParquet(TAOS_RES *res, const char *fileName, int64_t *outRows) {
    if (res == NULL || fileName == NULL) {
        logError("resultToFileParquet: invalid parameters");
        return TSDB_CODE_BCK_INVALID_PARAM;
    }

    int numFields = taos_num_fields(res);
    if (numFields <= 0) {
        logError("resultToFileParquet: no fields, errstr: %s", taos_errstr(res));
        return TSDB_CODE_BCK_NO_FIELDS;
    }

    TAOS_FIELD *fields = taos_fetch_fields(res);
    if (fields == NULL) {
        logError("resultToFileParquet: fetch fields failed, errstr: %s",
                 taos_errstr(res));
        return TSDB_CODE_BCK_FETCH_FIELDS_FAILED;
    }

    /* Extended fields carry precision/scale for DECIMAL columns. */
    TAOS_FIELD_E *efields = taos_fetch_fields_e(res);

    int code = TSDB_CODE_FAILED;
    ParquetWriter *pw = parquetWriterCreate(fileName, fields, numFields, efields, &code);
    if (pw == NULL) {
        return code;
    }

    int     blockRows  = 0;
    void   *block      = NULL;
    int64_t totalRows  = 0;

    while (taos_fetch_raw_block(res, &blockRows, &block) == TSDB_CODE_SUCCESS) {
        if (g_interrupted) {
            parquetWriterClose(pw);
            return TSDB_CODE_BCK_USER_CANCEL;
        }
        if (blockRows == 0 || block == NULL) break;
        totalRows += blockRows;

        code = parquetWriterWriteBlock(pw, block, blockRows);
        if (code != TSDB_CODE_SUCCESS) {
            logError("resultToFileParquet: write block failed(%d): %s",
                     code, fileName);
            parquetWriterClose(pw);
            return code;
        }
    }

    if (outRows) *outRows = totalRows;

    code = parquetWriterClose(pw);
    if (code != TSDB_CODE_SUCCESS) {
        logError("resultToFileParquet: close writer failed: %s", fileName);
    }
    return code;
}

/* ─────────────────────────────── restore ───────────────────────────*/

/*
 * Callback userData for the Parquet reader during restore.
 */
typedef struct {
    TAOS_STMT  *stmt;
    int64_t     totalRows;
    int64_t     batchRows;
    const char *tbName;   /* for error messages */
} ParStmtCtx;

/*
 * ParquetBindCallback: called once per row-group by parquetReaderReadAll.
 * bindArray[] entries are ready-to-use (memory owned by the reader).
 */
static int parStmtCallback(void          *userData,
                            TAOS_FIELD    *fields,
                            int            numFields,
                            TAOS_MULTI_BIND *bindArray,
                            int32_t        numRows) {
    (void)fields;
    (void)numFields;
    ParStmtCtx *ctx = (ParStmtCtx *)userData;

    if (g_interrupted) return TSDB_CODE_BCK_USER_CANCEL;
    if (numRows == 0)   return TSDB_CODE_SUCCESS;

    int ret = taos_stmt_bind_param_batch(ctx->stmt, bindArray);
    if (ret != 0) {
        logError("fileParquetToStmt: bind_param_batch failed: %s table=%s",
                 taos_stmt_errstr(ctx->stmt), ctx->tbName);
        return ret;
    }

    ret = taos_stmt_add_batch(ctx->stmt);
    if (ret != 0) {
        logError("fileParquetToStmt: add_batch failed: %s table=%s",
                 taos_stmt_errstr(ctx->stmt), ctx->tbName);
        return ret;
    }

    ctx->totalRows += numRows;
    ctx->batchRows += numRows;

    if (ctx->batchRows >= PAR_STMT_BATCH_THRESHOLD) {
        ret = taos_stmt_execute(ctx->stmt);
        if (ret != 0) {
            logError("fileParquetToStmt: execute failed: %s table=%s batchRows=%" PRId64,
                     taos_stmt_errstr(ctx->stmt), ctx->tbName, ctx->batchRows);
            return ret;
        }
        ctx->batchRows = 0;
    }

    return TSDB_CODE_SUCCESS;
}

/*
 * Restore a .par file into a table via a pre-prepared TAOS_STMT.
 *
 * The stmt must already be prepared with:
 *   INSERT INTO `db`.`tb` VALUES(?,?,?...)
 * with a placeholder count matching the column count in @fileName.
 */
int fileParquetToStmt(TAOS_STMT  *stmt,
                      const char *fileName,
                      int64_t    *outRows) {
    if (stmt == NULL || fileName == NULL) {
        logError("fileParquetToStmt: invalid parameters");
        return TSDB_CODE_BCK_INVALID_PARAM;
    }

    int code = TSDB_CODE_FAILED;
    ParquetReader *pr = parquetReaderOpen(fileName, &code);
    if (pr == NULL) {
        logError("fileParquetToStmt: open '%s' failed(%d)", fileName, code);
        return code;
    }

    /* Extract table name from path for error messages */
    const char *base = strrchr(fileName, '/');
    base = base ? base + 1 : fileName;

    ParStmtCtx ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.stmt   = stmt;
    ctx.tbName = base;

    code = parquetReaderReadAll(pr, parStmtCallback, &ctx);

    if (code == TSDB_CODE_SUCCESS && ctx.batchRows > 0) {
        /* flush final partial batch */
        int ret = taos_stmt_execute(stmt);
        if (ret != 0) {
            logError("fileParquetToStmt: final execute failed: %s file=%s",
                     taos_stmt_errstr(stmt), fileName);
            code = ret;
        }
    }

    if (outRows) *outRows = ctx.totalRows;

    parquetReaderClose(pr);
    return code;
}

/* ─────────────────────── restore via STMT2 ───────────────────────── */

typedef struct {
    TAOS_STMT2  *stmt2;
    int64_t      totalRows;
    const char  *tbName;
} ParStmt2Ctx;

/*
 * Callback invoked once per row-group by parquetReaderReadAll.
 * Converts the TAOS_MULTI_BIND array (STMT1 layout, owned by reader) to a
 * TAOS_STMT2_BIND array (STMT2 layout) and calls taos_stmt2_bind_param +
 * taos_stmt2_exec.  The conversion is O(numFields) and allocates only a
 * small array of bind descriptors on each call – no column data is copied.
 */
static int parStmt2Callback(void           *userData,
                             TAOS_FIELD     *fields,
                             int             numFields,
                             TAOS_MULTI_BIND *bindArray,
                             int32_t         numRows) {
    ParStmt2Ctx *ctx = (ParStmt2Ctx *)userData;
    if (g_interrupted) return TSDB_CODE_BCK_USER_CANCEL;
    if (numRows == 0)  return TSDB_CODE_SUCCESS;

    /* Convert TAOS_MULTI_BIND → TAOS_STMT2_BIND.
     * For DECIMAL columns, STMT2 requires the data as decimal text strings
     * (same format as the binary STMT2 restore path).
     * For all other types, buffer is passed through unchanged.               */
    TAOS_STMT2_BIND *binds2 =
        (TAOS_STMT2_BIND *)taosMemoryCalloc(numFields, sizeof(TAOS_STMT2_BIND));
    /* Per-field auxiliary buffers for DECIMAL string conversion.
     * Index i is non-NULL only for DECIMAL columns.                          */
    char    **decStrBufs = (char    **)taosMemoryCalloc(numFields, sizeof(char *));
    int32_t **decLens    = (int32_t **)taosMemoryCalloc(numFields, sizeof(int32_t *));
    if (!binds2 || !decStrBufs || !decLens) {
        taosMemoryFree(binds2);
        taosMemoryFree(decStrBufs);
        taosMemoryFree(decLens);
        return TSDB_CODE_BCK_MALLOC_FAILED;
    }

    int retCode = TSDB_CODE_SUCCESS;

    for (int i = 0; i < numFields; i++) {
        binds2[i].buffer_type = bindArray[i].buffer_type;
        binds2[i].buffer      = bindArray[i].buffer;
        binds2[i].length      = bindArray[i].length;
        binds2[i].is_null     = bindArray[i].is_null;
        binds2[i].num         = numRows;

        bool isDecimal = IS_DECIMAL_TYPE(fields[i].type);
        bool isVar     = IS_VAR_DATA_TYPE(fields[i].type) ||
                         fields[i].type == TSDB_DATA_TYPE_JSON      ||
                         fields[i].type == TSDB_DATA_TYPE_VARBINARY ||
                         fields[i].type == TSDB_DATA_TYPE_BLOB      ||
                         fields[i].type == TSDB_DATA_TYPE_MEDIUMBLOB||
                         fields[i].type == TSDB_DATA_TYPE_GEOMETRY;

        if (!isDecimal && !isVar) continue;  /* fixed-width: pass through */

        /* STMT2 reads variable-length rows at cumulative offsets:
         *   row r starts at buffer + sum(length[0..r-1])
         * The parquet reader provides fixed-stride buffers (row r at r*maxLen).
         * We must repack from fixed-stride → contiguous for ALL var-length types.
         * For DECIMAL, further convert raw binary bytes → decimal text string.   */
        int32_t stride = bindArray[i].buffer_length;  /* bytes per row (fixed stride) */

        /* ── For DECIMAL: first convert raw bytes → text strings in tmpBuf ── */
        char    *srcBuf  = NULL;    /* source data to repack (text for DECIMAL, raw for isVar) */
        int32_t *srcLens = NULL;    /* per-row source lengths */
        char    *tmpText = NULL;    /* allocated only for DECIMAL */
        int32_t *tmpLens = NULL;    /* allocated only for DECIMAL */

        if (isDecimal) {
            /* fields[i].bytes after recoverSchema = (actualBytes<<24)|(precision<<8)|scale */
            int32_t packedBytes = fields[i].bytes;
            int32_t hi          = (packedBytes >> 24) & 0xFF;
            int8_t  precision   = hi != 0 ? (int8_t)((packedBytes >> 8) & 0xFF) : 38;
            int8_t  scale       = hi != 0 ? (int8_t)( packedBytes        & 0xFF) : 0;
            int32_t maxStrLen   = (int32_t)precision + 10;

            tmpText = (char    *)taosMemoryCalloc(numRows, maxStrLen);
            tmpLens = (int32_t *)taosMemoryCalloc(numRows, sizeof(int32_t));
            if (!tmpText || !tmpLens) {
                taosMemoryFree(tmpText);
                taosMemoryFree(tmpLens);
                retCode = TSDB_CODE_BCK_MALLOC_FAILED;
                break;
            }
            const char *rawBuf = (const char *)bindArray[i].buffer;
            for (int r = 0; r < numRows; r++) {
                if (bindArray[i].is_null && bindArray[i].is_null[r]) {
                    tmpLens[r] = 0;
                } else {
                    const void *rawDec = rawBuf + (size_t)r * stride;
                    char *dest = tmpText + r * maxStrLen;
                    decimalToStr((const DecimalType *)rawDec, fields[i].type,
                                 precision, scale, dest, maxStrLen);
                    tmpLens[r] = (int32_t)strlen(dest);
                }
            }
            srcBuf  = tmpText;
            srcLens = tmpLens;
            stride  = maxStrLen;   /* stride into tmpText */
        } else {
            /* isVar: use existing fixed-stride buffer from parquet reader */
            srcBuf  = (char    *)bindArray[i].buffer;
            srcLens = (int32_t *)bindArray[i].length;
        }

        /* ── Repack: compute total bytes needed ── */
        int32_t totalLen = 0;
        for (int r = 0; r < numRows; r++) totalLen += srcLens ? srcLens[r] : 0;

        /* ── Alloc contiguous buffer + per-row length array ── */
        char    *contigBuf = (char    *)taosMemoryCalloc(1, totalLen > 0 ? totalLen : 1);
        int32_t *contigLen = (int32_t *)taosMemoryCalloc(numRows, sizeof(int32_t));
        if (!contigBuf || !contigLen) {
            taosMemoryFree(contigBuf);
            taosMemoryFree(contigLen);
            taosMemoryFree(tmpText);
            taosMemoryFree(tmpLens);
            retCode = TSDB_CODE_BCK_MALLOC_FAILED;
            break;
        }

        int32_t writeOff = 0;
        for (int r = 0; r < numRows; r++) {
            int32_t rlen = srcLens ? srcLens[r] : 0;
            contigLen[r] = rlen;
            if (rlen > 0) {
                memcpy(contigBuf + writeOff, srcBuf + (size_t)r * stride, rlen);
                writeOff += rlen;
            }
        }

        /* free temporary DECIMAL text buffer if allocated */
        taosMemoryFree(tmpText);
        taosMemoryFree(tmpLens);

        decStrBufs[i] = contigBuf;
        decLens[i]    = contigLen;

        binds2[i].buffer = contigBuf;
        binds2[i].length = contigLen;
    }

    int ret = TSDB_CODE_SUCCESS;
    if (retCode == TSDB_CODE_SUCCESS) {
        TAOS_STMT2_BINDV bv;
        memset(&bv, 0, sizeof(bv));
        bv.count     = 1;
        bv.tbnames   = NULL;     /* table name embedded in prepared SQL */
        bv.tags      = NULL;     /* child table already created */
        bv.bind_cols = &binds2;

        ret = taos_stmt2_bind_param(ctx->stmt2, &bv, -1);
        if (ret != 0) {
            logError("fileParquetToStmt2: bind_param failed: %s table=%s",
                     taos_stmt2_error(ctx->stmt2), ctx->tbName);
        }
    } else {
        ret = retCode;
    }

    /* Free DECIMAL conversion buffers */
    for (int i = 0; i < numFields; i++) {
        taosMemoryFree(decStrBufs[i]);
        taosMemoryFree(decLens[i]);
    }
    taosMemoryFree(decStrBufs);
    taosMemoryFree(decLens);
    taosMemoryFree(binds2);

    if (ret != 0) return ret;

    int affectedRows = 0;
    ret = taos_stmt2_exec(ctx->stmt2, &affectedRows);
    if (ret != 0) {
        logError("fileParquetToStmt2: exec failed: %s table=%s",
                 taos_stmt2_error(ctx->stmt2), ctx->tbName);
        return ret;
    }

    ctx->totalRows += affectedRows;
    return TSDB_CODE_SUCCESS;
}

int fileParquetToStmt2(TAOS_STMT2 *stmt2, const char *fileName, int64_t *outRows) {
    if (!stmt2 || !fileName) {
        logError("fileParquetToStmt2: invalid parameters");
        return TSDB_CODE_BCK_INVALID_PARAM;
    }

    int code = TSDB_CODE_FAILED;
    ParquetReader *pr = parquetReaderOpen(fileName, &code);
    if (!pr) {
        logError("fileParquetToStmt2: open '%s' failed(%d)", fileName, code);
        return code;
    }

    const char *base = strrchr(fileName, '/');
    base = base ? base + 1 : fileName;

    ParStmt2Ctx ctx;
    memset(&ctx, 0, sizeof(ctx));
    ctx.stmt2  = stmt2;
    ctx.tbName = base;

    code = parquetReaderReadAll(pr, parStmt2Callback, &ctx);

    if (outRows) *outRows = ctx.totalRows;
    parquetReaderClose(pr);
    return code;
}
