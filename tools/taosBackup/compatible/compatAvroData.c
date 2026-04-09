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

#include "compatAvroSchema.h"
#include "compatAvroUtil.h"
#include "compatAvro.h"
#include <avro.h>
// Temporarily undef TDengine atomic overrides for jansson.h inline functions
#pragma push_macro("__atomic_add_fetch")
#pragma push_macro("__atomic_sub_fetch")
#undef __atomic_add_fetch
#undef __atomic_sub_fetch
#include <jansson.h>
#pragma pop_macro("__atomic_sub_fetch")
#pragma pop_macro("__atomic_add_fetch")
#include "bck.h"


#ifndef TSDB_MAX_ALLOWED_SQL_LEN
#define TSDB_MAX_ALLOWED_SQL_LEN (4*1024*1024u)
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>

// ---- Column buffer helpers for STMT1 column-wise binding ----

// Maximum bytes per fixed-width type
static int typeFixedBytes(int type) {
    switch (type) {
        case TSDB_DATA_TYPE_BOOL:      return sizeof(int8_t);
        case TSDB_DATA_TYPE_TINYINT:   return sizeof(int8_t);
        case TSDB_DATA_TYPE_SMALLINT:  return sizeof(int16_t);
        case TSDB_DATA_TYPE_INT:       return sizeof(int32_t);
        case TSDB_DATA_TYPE_BIGINT:    return sizeof(int64_t);
        case TSDB_DATA_TYPE_FLOAT:     return sizeof(float);
        case TSDB_DATA_TYPE_DOUBLE:    return sizeof(double);
        case TSDB_DATA_TYPE_TIMESTAMP: return sizeof(int64_t);
        case TSDB_DATA_TYPE_UTINYINT:  return sizeof(uint8_t);
        case TSDB_DATA_TYPE_USMALLINT: return sizeof(uint16_t);
        case TSDB_DATA_TYPE_UINT:      return sizeof(uint32_t);
        case TSDB_DATA_TYPE_UBIGINT:   return sizeof(uint64_t);
        default:                       return 0;  // variable length
    }
}

static bool isVarLenType(int type) {
    return (type == TSDB_DATA_TYPE_BINARY   || type == TSDB_DATA_TYPE_NCHAR     ||
            type == TSDB_DATA_TYPE_JSON     || type == TSDB_DATA_TYPE_VARBINARY ||
            type == TSDB_DATA_TYPE_GEOMETRY || type == TSDB_DATA_TYPE_BLOB      ||
            type == TSDB_DATA_TYPE_DECIMAL  || type == TSDB_DATA_TYPE_DECIMAL64);
}

// Column buffer for accumulating rows before batch insert
typedef struct {
    int      type;        // TSDB_DATA_TYPE_*
    int      fixedSize;   // bytes per element for fixed types, max length for var types
    char    *data;        // data buffer
    char    *isNull;      // null flags (1 byte per row)
    int32_t *lengths;     // per-row lengths (var-len only)
    int      capacity;    // max rows
    int      rows;        // current accumulated rows
} ColBuf;

static ColBuf *allocColBufs(int nCols, int batchSize, AvroTableDes *tableDes) {
    ColBuf *bufs = (ColBuf *)taosMemoryCalloc(nCols, sizeof(ColBuf));
    if (!bufs) return NULL;

    for (int i = 0; i < nCols; i++) {
        bufs[i].type     = tableDes->cols[i].type;
        bufs[i].capacity = batchSize;
        bufs[i].rows     = 0;
        bufs[i].isNull   = (char *)taosMemoryCalloc(batchSize, sizeof(char));

        int fb = typeFixedBytes(bufs[i].type);
        if (fb > 0) {
            bufs[i].fixedSize = fb;
            bufs[i].data = (char *)taosMemoryCalloc(batchSize, fb);
        } else {
            // Variable length: allocate with max column length from tableDes
            int maxLen = tableDes->cols[i].length;
            if (maxLen <= 0) maxLen = 256;
            bufs[i].fixedSize = maxLen;
            bufs[i].data    = (char *)taosMemoryCalloc(batchSize, maxLen);
            bufs[i].lengths = (int32_t *)taosMemoryCalloc(batchSize, sizeof(int32_t));
        }

        if (!bufs[i].data || !bufs[i].isNull) {
            // Cleanup on failure
            for (int j = 0; j <= i; j++) {
                taosMemoryFree(bufs[j].data);
                taosMemoryFree(bufs[j].isNull);
                if (bufs[j].lengths) taosMemoryFree(bufs[j].lengths);
            }
            taosMemoryFree(bufs);
            return NULL;
        }
    }
    return bufs;
}

static void resetColBufs(ColBuf *bufs, int nCols) {
    for (int i = 0; i < nCols; i++) {
        memset(bufs[i].data, 0, (size_t)bufs[i].capacity * bufs[i].fixedSize);
        memset(bufs[i].isNull, 0, (size_t)bufs[i].capacity * sizeof(char));
        if (bufs[i].lengths)
            memset(bufs[i].lengths, 0, (size_t)bufs[i].capacity * sizeof(int32_t));
        bufs[i].rows = 0;
    }
}

static void freeColBufs(ColBuf *bufs, int nCols) {
    if (!bufs) return;
    for (int i = 0; i < nCols; i++) {
        taosMemoryFree(bufs[i].data);
        taosMemoryFree(bufs[i].isNull);
        if (bufs[i].lengths) taosMemoryFree(bufs[i].lengths);
    }
    taosMemoryFree(bufs);
}

// Fill TAOS_MULTI_BIND array from column buffers for batch binding
static void fillMultiBind(TAOS_MULTI_BIND *binds, ColBuf *bufs, int nCols, int nRows) {
    for (int i = 0; i < nCols; i++) {
        binds[i].buffer_type   = bufs[i].type;
        binds[i].buffer        = bufs[i].data;
        binds[i].buffer_length = bufs[i].fixedSize;
        binds[i].length        = bufs[i].lengths;  // NULL for fixed types
        binds[i].is_null       = bufs[i].isNull;
        binds[i].num           = nRows;
    }
}

// ---- Data extraction: AVRO value → column buffer row ----

static void extractDataTimestamp(AvroFieldInfo *fi, avro_value_t *val,
                                 ColBuf *cb, int row) {
    int64_t ts = 0;
    if (fi->nullable) {
        avro_value_t branch;
        avro_value_get_current_branch(val, &branch);
        if (avro_value_get_null(&branch) == 0) {
            cb->isNull[row] = 1;
            return;
        }
        avro_value_get_long(&branch, &ts);
    } else {
        avro_value_get_long(val, &ts);
    }
    memcpy(cb->data + (int64_t)row * cb->fixedSize, &ts, sizeof(int64_t));
}

static void extractDataBool(AvroFieldInfo *fi, avro_value_t *val,
                            ColBuf *cb, int row) {
    avro_value_t branch;
    avro_value_get_current_branch(val, &branch);
    if (avro_value_get_null(&branch) == 0) {
        cb->isNull[row] = 1;
        return;
    }
    int32_t bl = 0;
    avro_value_get_boolean(&branch, &bl);
    int8_t v = (int8_t)(bl ? 1 : 0);
    memcpy(cb->data + (int64_t)row * cb->fixedSize, &v, sizeof(int8_t));
}

static void extractDataTinyInt(AvroFieldInfo *fi, avro_value_t *val,
                               ColBuf *cb, int row) {
    if (fi->nullable) {
        avro_value_t branch;
        avro_value_get_current_branch(val, &branch);
        if (avro_value_get_null(&branch) == 0) { cb->isNull[row] = 1; return; }
        int32_t n = 0;
        avro_value_get_int(&branch, &n);
        int8_t v = (int8_t)n;
        memcpy(cb->data + (int64_t)row * cb->fixedSize, &v, sizeof(int8_t));
    } else {
        int32_t n = 0;
        avro_value_get_int(val, &n);
        if ((int8_t)n == (int8_t)TSDB_DATA_TINYINT_NULL) { cb->isNull[row] = 1; return; }
        int8_t v = (int8_t)n;
        memcpy(cb->data + (int64_t)row * cb->fixedSize, &v, sizeof(int8_t));
    }
}

static void extractDataSmallInt(AvroFieldInfo *fi, avro_value_t *val,
                                ColBuf *cb, int row) {
    if (fi->nullable) {
        avro_value_t branch;
        avro_value_get_current_branch(val, &branch);
        if (avro_value_get_null(&branch) == 0) { cb->isNull[row] = 1; return; }
        int32_t n = 0;
        avro_value_get_int(&branch, &n);
        int16_t v = (int16_t)n;
        memcpy(cb->data + (int64_t)row * cb->fixedSize, &v, sizeof(int16_t));
    } else {
        int32_t n = 0;
        avro_value_get_int(val, &n);
        if ((int16_t)n == (int16_t)TSDB_DATA_SMALLINT_NULL) { cb->isNull[row] = 1; return; }
        int16_t v = (int16_t)n;
        memcpy(cb->data + (int64_t)row * cb->fixedSize, &v, sizeof(int16_t));
    }
}

static void extractDataInt(AvroFieldInfo *fi, avro_value_t *val,
                           ColBuf *cb, int row) {
    if (fi->nullable) {
        avro_value_t branch;
        avro_value_get_current_branch(val, &branch);
        if (avro_value_get_null(&branch) == 0) { cb->isNull[row] = 1; return; }
        int32_t n = 0;
        avro_value_get_int(&branch, &n);
        if (n == (int32_t)TSDB_DATA_INT_NULL) { cb->isNull[row] = 1; return; }
        memcpy(cb->data + (int64_t)row * cb->fixedSize, &n, sizeof(int32_t));
    } else {
        int32_t n = 0;
        avro_value_get_int(val, &n);
        if (n == (int32_t)TSDB_DATA_INT_NULL) { cb->isNull[row] = 1; return; }
        memcpy(cb->data + (int64_t)row * cb->fixedSize, &n, sizeof(int32_t));
    }
}

static void extractDataBigInt(AvroFieldInfo *fi, avro_value_t *val,
                              ColBuf *cb, int row) {
    if (fi->nullable) {
        avro_value_t branch;
        avro_value_get_current_branch(val, &branch);
        if (avro_value_get_null(&branch) == 0) { cb->isNull[row] = 1; return; }
        int64_t n = 0;
        avro_value_get_long(&branch, &n);
        memcpy(cb->data + (int64_t)row * cb->fixedSize, &n, sizeof(int64_t));
    } else {
        int64_t n = 0;
        avro_value_get_long(val, &n);
        if (n == (int64_t)TSDB_DATA_BIGINT_NULL) { cb->isNull[row] = 1; return; }
        memcpy(cb->data + (int64_t)row * cb->fixedSize, &n, sizeof(int64_t));
    }
}

static void extractDataFloat(AvroFieldInfo *fi, avro_value_t *val,
                             ColBuf *cb, int row) {
    if (fi->nullable) {
        avro_value_t branch;
        avro_value_get_current_branch(val, &branch);
        if (avro_value_get_null(&branch) == 0) { cb->isNull[row] = 1; return; }
        float f = 0;
        avro_value_get_float(&branch, &f);
        memcpy(cb->data + (int64_t)row * cb->fixedSize, &f, sizeof(float));
    } else {
        float f = 0;
        avro_value_get_float(val, &f);
        if (f == TSDB_DATA_FLOAT_NULL) { cb->isNull[row] = 1; return; }
        memcpy(cb->data + (int64_t)row * cb->fixedSize, &f, sizeof(float));
    }
}

static void extractDataDouble(AvroFieldInfo *fi, avro_value_t *val,
                              ColBuf *cb, int row) {
    if (fi->nullable) {
        avro_value_t branch;
        avro_value_get_current_branch(val, &branch);
        if (avro_value_get_null(&branch) == 0) { cb->isNull[row] = 1; return; }
        double d = 0;
        avro_value_get_double(&branch, &d);
        memcpy(cb->data + (int64_t)row * cb->fixedSize, &d, sizeof(double));
    } else {
        double d = 0;
        avro_value_get_double(val, &d);
        if (d == TSDB_DATA_DOUBLE_NULL) { cb->isNull[row] = 1; return; }
        memcpy(cb->data + (int64_t)row * cb->fixedSize, &d, sizeof(double));
    }
}

static void extractDataBinary(AvroFieldInfo *fi, avro_value_t *val,
                              ColBuf *cb, int row) {
    avro_value_t branch;
    avro_value_get_current_branch(val, &branch);
    char *buf = NULL;
    size_t sz = 0;
    avro_value_get_string(&branch, (const char **)&buf, &sz);
    if (!buf || sz == 0) {
        cb->isNull[row] = 1;
        return;
    }
    // Remove trailing null from AVRO string
    if (sz > 0 && buf[sz - 1] == '\0') sz--;
    int32_t copyLen = (int32_t)sz;
    if (copyLen > cb->fixedSize) copyLen = cb->fixedSize;
    memcpy(cb->data + (int64_t)row * cb->fixedSize, buf, copyLen);
    if (cb->lengths) cb->lengths[row] = copyLen;
}

static void extractDataNChar(AvroFieldInfo *fi, avro_value_t *val,
                             ColBuf *cb, int row) {
    void *buf = NULL;
    size_t sz = 0;
    avro_value_t branch;
    avro_value_get_current_branch(val, &branch);
    avro_value_get_bytes(&branch, (const void **)&buf, &sz);
    if (!buf) {
        cb->isNull[row] = 1;
        return;
    }
    int32_t copyLen = (int32_t)sz;
    if (copyLen > cb->fixedSize) copyLen = cb->fixedSize;
    memcpy(cb->data + (int64_t)row * cb->fixedSize, buf, copyLen);
    if (cb->lengths) cb->lengths[row] = copyLen;
}

static void extractDataBytes(AvroFieldInfo *fi, avro_value_t *val,
                             ColBuf *cb, int row) {
    void *buf = NULL;
    size_t sz = 0;
    avro_value_t branch;
    avro_value_get_current_branch(val, &branch);
    avro_value_get_bytes(&branch, (const void **)&buf, &sz);
    if (!buf) {
        cb->isNull[row] = 1;
        return;
    }
    int32_t copyLen = (int32_t)sz;
    if (copyLen > cb->fixedSize) copyLen = cb->fixedSize;
    memcpy(cb->data + (int64_t)row * cb->fixedSize, buf, copyLen);
    if (cb->lengths) cb->lengths[row] = copyLen;
}

// Unsigned types from AVRO array encoding
static void extractDataUnsigned(AvroFieldInfo *fi, avro_value_t *val,
                                ColBuf *cb, int row, int bytes) {
    avro_value_t src = *val;
    bool fromBranch = false;

    if (fi->nullable) {
        avro_value_t branch;
        avro_value_get_current_branch(val, &branch);
        if (avro_value_get_null(&branch) == 0) {
            cb->isNull[row] = 1;
            return;
        }
        src = branch;
        fromBranch = true;
    }

    size_t arrSz = 0;
    avro_value_get_size(&src, &arrSz);

    if (bytes <= 4) {
        uint64_t sum = 0;
        for (size_t j = 0; j < arrSz; j++) {
            avro_value_t item;
            avro_value_get_by_index(&src, j, &item, NULL);
            int32_t n = 0;
            avro_value_get_int(&item, &n);
            sum += (uint32_t)n;
        }
        if (bytes == 1) {
            uint8_t v = (uint8_t)sum;
            memcpy(cb->data + (int64_t)row * cb->fixedSize, &v, sizeof(uint8_t));
        } else if (bytes == 2) {
            uint16_t v = (uint16_t)sum;
            memcpy(cb->data + (int64_t)row * cb->fixedSize, &v, sizeof(uint16_t));
        } else {
            uint32_t v = (uint32_t)sum;
            memcpy(cb->data + (int64_t)row * cb->fixedSize, &v, sizeof(uint32_t));
        }
    } else {
        uint64_t sum = 0;
        for (size_t j = 0; j < arrSz; j++) {
            avro_value_t item;
            avro_value_get_by_index(&src, j, &item, NULL);
            int64_t n = 0;
            avro_value_get_long(&item, &n);
            sum += (uint64_t)n;
        }
        memcpy(cb->data + (int64_t)row * cb->fixedSize, &sum, sizeof(uint64_t));
    }
}

// ==================== Core data import ====================

// Execute a batch: bind_param_batch → add_batch → execute
static int executeBatch(TAOS_STMT *stmt, ColBuf *bufs, int nCols, int nRows) {
    TAOS_MULTI_BIND *binds = (TAOS_MULTI_BIND *)taosMemoryCalloc(nCols, sizeof(TAOS_MULTI_BIND));
    if (!binds) return -1;

    fillMultiBind(binds, bufs, nCols, nRows);

    int code = taos_stmt_bind_param_batch(stmt, binds);
    if (code != 0) {
        logError("avro data: bind_param_batch failed: %s", taos_stmt_errstr(stmt));
        taosMemoryFree(binds);
        return code;
    }

    code = taos_stmt_add_batch(stmt);
    if (code != 0) {
        logError("avro data: add_batch failed: %s", taos_stmt_errstr(stmt));
        taosMemoryFree(binds);
        return code;
    }

    code = taos_stmt_execute(stmt);
    if (code != 0) {
        logError("avro data: execute failed: %s", taos_stmt_errstr(stmt));
    }

    taosMemoryFree(binds);
    return code;
}

int64_t avroRestoreDataImpl(AvroRestoreCtx *ctx,
                            TAOS *conn,
                            TAOS_STMT *stmt,
                            const char *dirPath,
                            const char *fileName,
                            AvroDBChange *pDbChange,
                            AvroStbChange *stbChange) {
    char avroFile[MAX_PATH_LEN];
    snprintf(avroFile, sizeof(avroFile), "%s/%s", dirPath, fileName);

    avro_file_reader_t reader;
    avro_schema_t schema;
    AvroRecordSchema *rs = avroGetSchemaFromFile(AVRO_TYPE_DATA, avroFile,
                                                  &schema, &reader);
    if (!rs) return -1;

    // Determine tableDes
    AvroTableDes *tableDes = stbChange ? stbChange->tableDes : NULL;
    AvroTableDes *mallocDes = NULL;

    // Compute bind column count
    int colAdj    = ctx->looseMode ? 0 : 1;
    int nBindCols = rs->numFields - colAdj;
    if (stbChange && stbChange->schemaChanged) {
        nBindCols = stbChange->tableDes->columns;
    }

    int batchSize = ctx->dataBatch;
    if (batchSize <= 0) batchSize = 60000;

    // AVRO reader loop
    avro_value_iface_t *iface = avro_generic_class_from_schema(schema);
    avro_value_t value;
    avro_generic_value_new(iface, &value);

    int64_t totalRows = 0;
    int64_t failed    = 0;
    char   *tbName    = NULL;
    bool    prepared  = false;
    ColBuf *colBufs   = NULL;

    while (!avro_file_reader_read_value(reader, &value)) {
        // Extract tbName on first record
        if (!tbName) {
            if (!ctx->looseMode) {
                avro_value_t tbVal, tbBranch;
                avro_value_get_by_name(&value, "tbname", &tbVal, NULL);
                avro_value_get_current_branch(&tbVal, &tbBranch);
                char *avroName = NULL;
                size_t tbSz;
                avro_value_get_string(&tbBranch, (const char **)&avroName, &tbSz);
                tbName = taosStrdup(avroName);
            } else {
                tbName = (char *)taosMemoryCalloc(1, AVRO_TABLE_NAME_LEN + 1);
                char *dup = taosStrdup(fileName);
                char *run = dup;
                strsep(&run, ".");
                char *tb = strsep(&run, ".");
                if (tb) snprintf(tbName, AVRO_TABLE_NAME_LEN, "%s", tb);
                taosMemoryFree(dup);
            }

            // Handle normal table stbChange lookup
            if (!stbChange && avroIsNormalTableFolder(dirPath)) {
                stbChange = avroFindStbChange(pDbChange, tbName);
                if (stbChange) {
                    nBindCols = stbChange->tableDes->columns;
                    tableDes  = stbChange->tableDes;
                }
            }

            // Get tableDes if not available
            if (!tableDes) {
                if (rs->tableDes) {
                    tableDes = rs->tableDes;
                } else {
                    mallocDes = avroAllocTableDes(TSDB_MAX_COLUMNS);
                    if (!mallocDes) goto cleanup;
                    tableDes = mallocDes;
                }
            }

            if (mallocDes && (strlen(tableDes->name) == 0
                    || strcmp(tableDes->name, tbName) != 0)) {
                if (avroGetTableDes(conn, ctx->targetDb, tbName, mallocDes, true) < 0) {
                    goto cleanup;
                }
            }

            // Prepare STMT1
            char stmtBuf[TSDB_MAX_ALLOWED_SQL_LEN];
            char *p = stmtBuf;
            p += snprintf(p, sizeof(stmtBuf), "INSERT INTO ? VALUES(?");
            for (int i = 1; i < nBindCols; i++) {
                p += sprintf(p, ",?");
            }
            sprintf(p, ")");

            if (taos_stmt_prepare(stmt, stmtBuf, 0) != 0) {
                logError("avro data: stmt prepare failed: %s", taos_stmt_errstr(stmt));
                goto cleanup;
            }

            // Set table name
            char escapedTb[AVRO_DB_NAME_LEN + AVRO_TABLE_NAME_LEN + 10];
            snprintf(escapedTb, sizeof(escapedTb), "`%s`.`%s`", ctx->targetDb, tbName);
            if (taos_stmt_set_tbname(stmt, escapedTb) != 0) {
                logError("avro data: set_tbname failed for %s: %s", escapedTb, taos_stmt_errstr(stmt));
                goto cleanup;
            }

            // Allocate column buffers
            colBufs = allocColBufs(nBindCols, batchSize, tableDes);
            if (!colBufs) {
                logError("avro data: alloc column buffers failed");
                goto cleanup;
            }

            prepared = true;
        }

        if (!prepared) continue;

        int rowIdx = colBufs[0].rows;

        // Extract each column
        int n = 0;
        for (int i = 0; i < rs->numFields - colAdj; i++) {
            AvroFieldInfo *fi = &rs->fields[i + colAdj];

            // Schema evolution filter
            if (stbChange && stbChange->schemaChanged) {
                if (!avroIdxInBindCols((int16_t)i, stbChange->tableDes))
                    continue;
            }

            avro_value_t fieldVal;
            if (i == 0) {
                // First column is always timestamp
                avro_value_get_by_name(&value, fi->name, &fieldVal, NULL);
                extractDataTimestamp(fi, &fieldVal, &colBufs[n], rowIdx);
            } else if (avro_value_get_by_name(&value, fi->name, &fieldVal, NULL) == 0) {
                int colType = tableDes->cols[n].type;
                switch (colType) {
                    case TSDB_DATA_TYPE_BOOL:
                        extractDataBool(fi, &fieldVal, &colBufs[n], rowIdx);
                        break;
                    case TSDB_DATA_TYPE_TINYINT:
                        extractDataTinyInt(fi, &fieldVal, &colBufs[n], rowIdx);
                        break;
                    case TSDB_DATA_TYPE_SMALLINT:
                        extractDataSmallInt(fi, &fieldVal, &colBufs[n], rowIdx);
                        break;
                    case TSDB_DATA_TYPE_INT:
                        extractDataInt(fi, &fieldVal, &colBufs[n], rowIdx);
                        break;
                    case TSDB_DATA_TYPE_BIGINT:
                        extractDataBigInt(fi, &fieldVal, &colBufs[n], rowIdx);
                        break;
                    case TSDB_DATA_TYPE_TIMESTAMP:
                        extractDataTimestamp(fi, &fieldVal, &colBufs[n], rowIdx);
                        break;
                    case TSDB_DATA_TYPE_FLOAT:
                        extractDataFloat(fi, &fieldVal, &colBufs[n], rowIdx);
                        break;
                    case TSDB_DATA_TYPE_DOUBLE:
                        extractDataDouble(fi, &fieldVal, &colBufs[n], rowIdx);
                        break;
                    case TSDB_DATA_TYPE_BINARY:
                    case TSDB_DATA_TYPE_DECIMAL:
                    case TSDB_DATA_TYPE_DECIMAL64:
                        extractDataBinary(fi, &fieldVal, &colBufs[n], rowIdx);
                        break;
                    case TSDB_DATA_TYPE_NCHAR:
                    case TSDB_DATA_TYPE_JSON:
                        extractDataNChar(fi, &fieldVal, &colBufs[n], rowIdx);
                        break;
                    case TSDB_DATA_TYPE_VARBINARY:
                    case TSDB_DATA_TYPE_GEOMETRY:
                    case TSDB_DATA_TYPE_BLOB:
                        extractDataBytes(fi, &fieldVal, &colBufs[n], rowIdx);
                        break;
                    case TSDB_DATA_TYPE_UTINYINT:
                        extractDataUnsigned(fi, &fieldVal, &colBufs[n], rowIdx, 1);
                        break;
                    case TSDB_DATA_TYPE_USMALLINT:
                        extractDataUnsigned(fi, &fieldVal, &colBufs[n], rowIdx, 2);
                        break;
                    case TSDB_DATA_TYPE_UINT:
                        extractDataUnsigned(fi, &fieldVal, &colBufs[n], rowIdx, 4);
                        break;
                    case TSDB_DATA_TYPE_UBIGINT:
                        extractDataUnsigned(fi, &fieldVal, &colBufs[n], rowIdx, 8);
                        break;
                    default:
                        colBufs[n].isNull[rowIdx] = 1;
                        break;
                }
            } else {
                colBufs[n].isNull[rowIdx] = 1;
            }
            n++;
        }

        // Advance row counter for all columns
        for (int c = 0; c < nBindCols; c++) {
            colBufs[c].rows = rowIdx + 1;
        }
        totalRows++;

        // Execute batch when full
        if (colBufs[0].rows >= batchSize) {
            int code = executeBatch(stmt, colBufs, nBindCols, colBufs[0].rows);
            if (code != 0) {
                failed += colBufs[0].rows;
            }
            resetColBufs(colBufs, nBindCols);
        }
    }

    // Flush remaining rows
    if (colBufs && colBufs[0].rows > 0) {
        int code = executeBatch(stmt, colBufs, nBindCols, colBufs[0].rows);
        if (code != 0) {
            failed += colBufs[0].rows;
        }
    }

cleanup:
    if (colBufs) freeColBufs(colBufs, nBindCols);
    if (tbName) taosMemoryFree(tbName);
    if (mallocDes) avroFreeTableDes(mallocDes, true);
    avro_value_decref(&value);
    avro_value_iface_decref(iface);
    avroFreeRecordSchema(rs);
    avro_file_reader_close(reader);

    if (failed > 0) return -(int64_t)failed;
    return totalRows;
}
