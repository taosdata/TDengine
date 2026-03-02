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
int resultToFileParquet(TAOS_RES *res, const char *fileName) {
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

    int code = TSDB_CODE_FAILED;
    ParquetWriter *pw = parquetWriterCreate(fileName, fields, numFields, &code);
    if (pw == NULL) {
        return code;
    }

    int    blockRows = 0;
    void  *block     = NULL;

    while (taos_fetch_raw_block(res, &blockRows, &block) == TSDB_CODE_SUCCESS) {
        if (g_interrupted) {
            parquetWriterClose(pw);
            return TSDB_CODE_BCK_USER_CANCEL;
        }
        if (blockRows == 0 || block == NULL) break;

        code = parquetWriterWriteBlock(pw, block, blockRows);
        if (code != TSDB_CODE_SUCCESS) {
            logError("resultToFileParquet: write block failed(%d): %s",
                     code, fileName);
            parquetWriterClose(pw);
            return code;
        }
    }

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
        return TSDB_CODE_BCK_STMT_FAILED;
    }

    ret = taos_stmt_add_batch(ctx->stmt);
    if (ret != 0) {
        logError("fileParquetToStmt: add_batch failed: %s table=%s",
                 taos_stmt_errstr(ctx->stmt), ctx->tbName);
        return TSDB_CODE_BCK_STMT_FAILED;
    }

    ctx->totalRows += numRows;
    ctx->batchRows += numRows;

    if (ctx->batchRows >= PAR_STMT_BATCH_THRESHOLD) {
        ret = taos_stmt_execute(ctx->stmt);
        if (ret != 0) {
            logError("fileParquetToStmt: execute failed: %s table=%s batchRows=%" PRId64,
                     taos_stmt_errstr(ctx->stmt), ctx->tbName, ctx->batchRows);
            return TSDB_CODE_BCK_STMT_FAILED;
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
            code = TSDB_CODE_BCK_STMT_FAILED;
        }
    }

    if (outRows) *outRows = ctx.totalRows;

    parquetReaderClose(pr);
    return code;
}
